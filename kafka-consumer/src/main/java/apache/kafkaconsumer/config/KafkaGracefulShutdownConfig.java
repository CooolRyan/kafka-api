package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;

/**
 * Kafka 4.1 CloseOptions를 사용한 Graceful Shutdown
 */
@Component
@Slf4j
public class KafkaGracefulShutdownConfig implements ApplicationListener<ContextClosedEvent> {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    public KafkaGracefulShutdownConfig(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    public void onApplicationEvent(ContextClosedEvent event) {
        log.info("🛑 Kafka 4.1 CloseOptions를 사용하여 graceful shutdown 시작...");
        
        Collection<MessageListenerContainer> containers = kafkaListenerEndpointRegistry.getAllListenerContainers();
        
        for (MessageListenerContainer container : containers) {
            if (container.isRunning()) {
                log.info("📦 Listener Container '{}' 종료 중...", container.getListenerId());
                
                try {
                    if (container instanceof ConcurrentMessageListenerContainer) {
                        stopContainerWithCloseOptions((ConcurrentMessageListenerContainer) container, Duration.ofSeconds(60));
                    } else {
                        container.stop();
                    }
                    
                    log.info("✅ Listener Container '{}' 종료 완료", container.getListenerId());
                } catch (Exception e) {
                    log.error("❌ Listener Container '{}' 종료 중 오류: {}", 
                        container.getListenerId(), e.getMessage(), e);
                    try {
                        container.stop();
                    } catch (Exception ex) {
                        log.error("❌ Fallback stop()도 실패: {}", ex.getMessage());
                    }
                }
            }
        }
        
        log.info("🎯 모든 Kafka Consumer graceful shutdown 완료");
    }

    private void stopContainerWithCloseOptions(ConcurrentMessageListenerContainer container, Duration timeout) {
        try {
            Field containersField = ConcurrentMessageListenerContainer.class.getDeclaredField("containers");
            containersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Collection<KafkaMessageListenerContainer<?, ?>> containers = 
                (Collection<KafkaMessageListenerContainer<?, ?>>) containersField.get(container);
            
            for (KafkaMessageListenerContainer<?, ?> kafkaContainer : containers) {
                if (!kafkaContainer.isRunning()) {
                    continue;
                }
                
                Consumer<?, ?> consumer = getConsumerFromContainer(kafkaContainer);
                
                if (consumer != null) {
                    closeConsumerWithOptions(consumer, timeout);
                    log.info("✅ Consumer.close(CloseOptions) 호출 완료");
                } else {
                    kafkaContainer.stop();
                }
            }
            
        } catch (Exception e) {
            log.error("❌ Container 종료 중 오류: {}", e.getMessage(), e);
            container.stop();
        }
    }
    
    @SuppressWarnings("unchecked")
    private Consumer<?, ?> getConsumerFromContainer(KafkaMessageListenerContainer<?, ?> container) {
        try {
            Field listenerConsumerField = KafkaMessageListenerContainer.class.getDeclaredField("listenerConsumer");
            listenerConsumerField.setAccessible(true);
            Object listenerConsumer = listenerConsumerField.get(container);
            
            if (listenerConsumer != null) {
                Field consumerField = listenerConsumer.getClass().getDeclaredField("consumer");
                consumerField.setAccessible(true);
                return (Consumer<?, ?>) consumerField.get(listenerConsumer);
            }
        } catch (Exception e) {
            log.debug("리플렉션으로 consumer 접근 실패: {}", e.getMessage());
        }
        return null;
    }
    
    private void closeConsumerWithOptions(Consumer<?, ?> consumer, Duration timeout) {
        try {
            Class<?> closeOptionsClass = Class.forName("org.apache.kafka.clients.consumer.Consumer$CloseOptions");
            Class<?> groupMembershipOperationEnum = Class.forName(
                "org.apache.kafka.clients.consumer.Consumer$CloseOptions$GroupMembershipOperation");
            
            Object remainInGroup = Enum.valueOf((Class<Enum>) groupMembershipOperationEnum, "REMAIN_IN_GROUP");
            
            Method timeoutMethod = closeOptionsClass.getMethod("timeout", Duration.class);
            Object closeOptions = timeoutMethod.invoke(null, timeout);
            
            Method withGroupMembershipOperation = closeOptionsClass.getMethod(
                "withGroupMembershipOperation", groupMembershipOperationEnum);
            closeOptions = withGroupMembershipOperation.invoke(closeOptions, remainInGroup);
            
            Method closeMethod = consumer.getClass().getMethod("close", closeOptionsClass);
            closeMethod.invoke(consumer, closeOptions);
            
            log.info("✅ Consumer.close(CloseOptions) 호출 완료");
            log.info("   - GroupMembershipOperation: REMAIN_IN_GROUP");
            log.info("   - Timeout: {}초", timeout.getSeconds());
            
        } catch (ClassNotFoundException e) {
            log.warn("⚠️ Kafka 4.1 CloseOptions를 찾을 수 없습니다. 기본 consumer.close() 사용");
            consumer.close(timeout);
        } catch (Exception e) {
            log.error("❌ CloseOptions 사용 중 오류: {}", e.getMessage(), e);
            consumer.close(timeout);
        }
    }
}

