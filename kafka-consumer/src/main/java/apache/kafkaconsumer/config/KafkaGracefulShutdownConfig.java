package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;

/**
 * Kafka 4.1 Graceful Shutdown Configuration
 * 
 * KIP-1092: Extend Consumer#close with an option to leave the group or not
 * https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=321719077
 * 
 * 롤링 업데이트 시 consumer group에서 즉시 leave되지 않도록 설정
 * - REMAIN_IN_GROUP: consumer group에 머물러 있어 리밸런싱 방지
 * - timeout: 60초 (롤링 업데이트 완료까지 대기)
 */
@Component
@Slf4j
public class KafkaGracefulShutdownConfig implements ApplicationListener<ContextClosedEvent> {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    public KafkaGracefulShutdownConfig(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        log.info("🛑 Spring Context가 종료됩니다. Kafka Consumer를 graceful하게 종료합니다...");
        
        // 모든 Kafka Listener Container 중지
        Collection<MessageListenerContainer> containers = kafkaListenerEndpointRegistry.getAllListenerContainers();
        
        for (MessageListenerContainer container : containers) {
            if (container.isRunning()) {
                log.info("📦 Listener Container '{}' 종료 중...", container.getListenerId());
                
                try {
                    // Kafka 4.1 CloseOptions 사용
                    // REMAIN_IN_GROUP: consumer group에 머물러 있어 리밸런싱 방지
                    // timeout: 60초 (롤링 업데이트 완료까지 대기)
                    if (container instanceof ConcurrentMessageListenerContainer) {
                        ConcurrentMessageListenerContainer concurrentContainer = 
                            (ConcurrentMessageListenerContainer) container;
                        
                        // 내부 consumer에 CloseOptions 적용
                        stopContainerWithCloseOptions(concurrentContainer, Duration.ofSeconds(60));
                    } else {
                        // 일반적인 경우 기본 stop 사용
                        container.stop();
                    }
                    
                    log.info("✅ Listener Container '{}' 종료 완료", container.getListenerId());
                } catch (Exception e) {
                    log.error("❌ Listener Container '{}' 종료 중 오류 발생: {}", 
                        container.getListenerId(), e.getMessage(), e);
                }
            }
        }
        
        log.info("🎯 모든 Kafka Consumer graceful shutdown 완료");
    }

    /**
     * Kafka 4.1 CloseOptions를 사용하여 Container 종료
     * 
     * KIP-1092: Consumer#close(CloseOptions) 사용
     * - REMAIN_IN_GROUP: consumer group에 머물러 있어 리밸런싱 방지
     * - timeout: 60초 (롤링 업데이트 완료까지 대기)
     * 
     * 참고: Spring Kafka의 내부 구조상 리플렉션을 사용하여
     * 내부 consumer에 접근하고 CloseOptions를 적용합니다.
     */
    private void stopContainerWithCloseOptions(
            ConcurrentMessageListenerContainer container, 
            Duration timeout) {
        
        try {
            log.info("🔄 Kafka 4.1 CloseOptions를 사용하여 graceful shutdown 시작...");
            log.info("   - GroupMembershipOperation: REMAIN_IN_GROUP");
            log.info("   - Timeout: {}초", timeout.getSeconds());
            
            // ConcurrentMessageListenerContainer는 여러 KafkaMessageListenerContainer를 포함
            // 각각의 container에 대해 CloseOptions 적용
            Field containersField = ConcurrentMessageListenerContainer.class.getDeclaredField("containers");
            containersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Collection<KafkaMessageListenerContainer<?, ?>> containers = 
                (Collection<KafkaMessageListenerContainer<?, ?>>) containersField.get(container);
            
            for (KafkaMessageListenerContainer<?, ?> kafkaContainer : containers) {
                try {
                    // 내부 consumer에 접근
                    Consumer<?, ?> consumer = getConsumerFromContainer(kafkaContainer);
                    
                    if (consumer != null) {
                        // Kafka 4.1 CloseOptions 사용
                        closeConsumerWithOptions(consumer, timeout);
                        log.info("✅ Consumer graceful shutdown 완료");
                    } else {
                        log.warn("⚠️ Consumer를 찾을 수 없습니다. 기본 stop() 사용");
                        kafkaContainer.stop();
                    }
                } catch (Exception e) {
                    log.error("❌ Consumer 종료 중 오류: {}", e.getMessage(), e);
                    kafkaContainer.stop();
                }
            }
            
        } catch (Exception e) {
            log.error("❌ Container 종료 중 오류 발생: {}", e.getMessage(), e);
            // Fallback: 기본 stop() 사용
            container.stop();
        }
    }
    
    /**
     * KafkaMessageListenerContainer에서 내부 consumer 추출 (리플렉션 사용)
     */
    @SuppressWarnings("unchecked")
    private Consumer<?, ?> getConsumerFromContainer(KafkaMessageListenerContainer<?, ?> container) {
        try {
            // Spring Kafka의 내부 구조에 따라 consumer 필드 접근
            // KafkaMessageListenerContainer는 내부적으로 ListenerConsumer를 가지고 있고,
            // ListenerConsumer는 consumer를 가지고 있습니다.
            
            Field listenerConsumerField = KafkaMessageListenerContainer.class.getDeclaredField("listenerConsumer");
            listenerConsumerField.setAccessible(true);
            Object listenerConsumer = listenerConsumerField.get(container);
            
            if (listenerConsumer != null) {
                // ListenerConsumer에서 consumer 필드 접근
                Field consumerField = listenerConsumer.getClass().getDeclaredField("consumer");
                consumerField.setAccessible(true);
                return (Consumer<?, ?>) consumerField.get(listenerConsumer);
            }
        } catch (Exception e) {
            log.debug("리플렉션으로 consumer 접근 실패: {}", e.getMessage());
        }
        return null;
    }
    
    /**
     * Kafka 4.1 CloseOptions를 사용하여 consumer 종료
     */
    private void closeConsumerWithOptions(Consumer<?, ?> consumer, Duration timeout) {
        try {
            // Kafka 4.1 CloseOptions 클래스 접근
            Class<?> closeOptionsClass = Class.forName("org.apache.kafka.clients.consumer.Consumer$CloseOptions");
            Class<?> groupMembershipOperationEnum = Class.forName(
                "org.apache.kafka.clients.consumer.Consumer$CloseOptions$GroupMembershipOperation");
            
            // GroupMembershipOperation.REMAIN_IN_GROUP 값 가져오기
            Object remainInGroup = Enum.valueOf((Class<Enum>) groupMembershipOperationEnum, "REMAIN_IN_GROUP");
            
            // CloseOptions 인스턴스 생성
            Object closeOptions = closeOptionsClass.getDeclaredConstructor().newInstance();
            
            // withGroupMembershipOperation() 메서드 호출
            Method withGroupMembershipOperation = closeOptionsClass.getMethod(
                "withGroupMembershipOperation", groupMembershipOperationEnum);
            closeOptions = withGroupMembershipOperation.invoke(closeOptions, remainInGroup);
            
            // withTimeout() 메서드 호출
            Method withTimeout = closeOptionsClass.getMethod("withTimeout", Duration.class);
            closeOptions = withTimeout.invoke(closeOptions, timeout);
            
            // consumer.close(CloseOptions) 호출
            Method closeMethod = consumer.getClass().getMethod("close", closeOptionsClass);
            closeMethod.invoke(consumer, closeOptions);
            
            log.info("✅ Consumer.close(CloseOptions) 호출 완료 - REMAIN_IN_GROUP, timeout: {}초", 
                timeout.getSeconds());
            
        } catch (ClassNotFoundException e) {
            log.warn("⚠️ Kafka 4.1 CloseOptions를 찾을 수 없습니다. Kafka 4.1+ 버전이 필요합니다.");
            log.warn("   기본 consumer.close() 사용");
            consumer.close(timeout);
        } catch (Exception e) {
            log.error("❌ CloseOptions 사용 중 오류: {}", e.getMessage(), e);
            // Fallback: 기본 close() 사용
            consumer.close(timeout);
        }
    }
}

