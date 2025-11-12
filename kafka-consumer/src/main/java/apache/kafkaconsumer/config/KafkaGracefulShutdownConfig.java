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
 * Kafka 4.1 Graceful Shutdown Configuration
 * 
 * KIP-1092: Extend Consumer#close with an option to leave the group or not
 * https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=321719077
 * 
 * Kafka 4.1 CloseOptions를 사용하여 consumer group에 머물러 있도록 설정
 * - REMAIN_IN_GROUP: consumer group에 머물러 있어 리밸런싱 방지
 * - timeout: 60초 (롤링 업데이트 완료까지 대기)
 * 
 * Static Group Membership (group.instance.id)과 함께 사용하면
 * 롤링 업데이트 시 파티션 할당이 유지됨
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
        log.info("🛑 Spring Context가 종료됩니다. Kafka 4.1 CloseOptions를 사용하여 graceful shutdown 시작...");
        
        // 모든 Kafka Listener Container 중지
        Collection<MessageListenerContainer> containers = kafkaListenerEndpointRegistry.getAllListenerContainers();
        
        for (MessageListenerContainer container : containers) {
            if (container.isRunning()) {
                log.info("📦 Listener Container '{}' 종료 중...", container.getListenerId());
                
                try {
                    // Kafka 4.1 CloseOptions 사용
                    if (container instanceof ConcurrentMessageListenerContainer) {
                        ConcurrentMessageListenerContainer concurrentContainer = 
                            (ConcurrentMessageListenerContainer) container;
                        
                        // CloseOptions를 사용하여 종료
                        stopContainerWithCloseOptions(concurrentContainer, Duration.ofSeconds(60));
                    } else {
                        // 일반적인 경우 기본 stop 사용
                        container.stop();
                    }
                    
                    log.info("✅ Listener Container '{}' 종료 완료", container.getListenerId());
                } catch (Exception e) {
                    log.error("❌ Listener Container '{}' 종료 중 오류 발생: {}", 
                        container.getListenerId(), e.getMessage(), e);
                    // Fallback: 기본 stop 사용
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

    /**
     * Kafka 4.1 CloseOptions를 사용하여 Container 종료
     * 
     * 주의: Container가 이미 stop 중이면 접근하지 않음
     */
    private void stopContainerWithCloseOptions(
            ConcurrentMessageListenerContainer container, 
            Duration timeout) {
        
        try {
            log.info("🔄 Kafka 4.1 CloseOptions를 사용하여 graceful shutdown 시작...");
            log.info("   - GroupMembershipOperation: REMAIN_IN_GROUP");
            log.info("   - Timeout: {}초", timeout.getSeconds());
            
            // Container가 이미 stop 중이 아닌지 확인
            if (!container.isRunning()) {
                log.warn("⚠️ Container가 이미 종료 중입니다. skip");
                return;
            }
            
            // ConcurrentMessageListenerContainer는 여러 KafkaMessageListenerContainer를 포함
            Field containersField = ConcurrentMessageListenerContainer.class.getDeclaredField("containers");
            containersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Collection<KafkaMessageListenerContainer<?, ?>> containers = 
                (Collection<KafkaMessageListenerContainer<?, ?>>) containersField.get(container);
            
            for (KafkaMessageListenerContainer<?, ?> kafkaContainer : containers) {
                try {
                    // Container가 실행 중일 때만 consumer에 접근
                    if (!kafkaContainer.isRunning()) {
                        log.debug("Container {}가 이미 종료됨, skip", kafkaContainer.getListenerId());
                        continue;
                    }
                    
                    // 내부 consumer에 접근 (안전하게)
                    Consumer<?, ?> consumer = getConsumerFromContainerSafely(kafkaContainer);
                    
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
                    // Fallback: 기본 stop 사용
                    try {
                        kafkaContainer.stop();
                    } catch (Exception ex) {
                        log.error("❌ Fallback stop()도 실패: {}", ex.getMessage());
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("❌ Container 종료 중 오류 발생: {}", e.getMessage(), e);
            // Fallback: 기본 stop() 사용
            try {
                container.stop();
            } catch (Exception ex) {
                log.error("❌ Fallback stop()도 실패: {}", ex.getMessage());
            }
        }
    }
    
    /**
     * KafkaMessageListenerContainer에서 내부 consumer 추출 (안전하게)
     * Container가 실행 중일 때만 접근
     */
    @SuppressWarnings("unchecked")
    private Consumer<?, ?> getConsumerFromContainerSafely(KafkaMessageListenerContainer<?, ?> container) {
        try {
            // Container가 실행 중인지 확인
            if (!container.isRunning()) {
                return null;
            }
            
            // Spring Kafka의 내부 구조에 따라 consumer 필드 접근
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
     * 
     * 예제:
     * consumer.close(CloseOptions.timeout(Duration.ofSeconds(60))
     *     .withGroupMembershipOperation(GroupMembershipOperation.REMAIN_IN_GROUP));
     */
    private void closeConsumerWithOptions(Consumer<?, ?> consumer, Duration timeout) {
        try {
            // Kafka 4.1 CloseOptions 클래스 접근
            Class<?> closeOptionsClass = Class.forName("org.apache.kafka.clients.consumer.Consumer$CloseOptions");
            Class<?> groupMembershipOperationEnum = Class.forName(
                "org.apache.kafka.clients.consumer.Consumer$CloseOptions$GroupMembershipOperation");
            
            // GroupMembershipOperation.REMAIN_IN_GROUP 값 가져오기
            Object remainInGroup = Enum.valueOf((Class<Enum>) groupMembershipOperationEnum, "REMAIN_IN_GROUP");
            
            // CloseOptions.timeout(Duration) static factory method 사용
            Method timeoutMethod = closeOptionsClass.getMethod("timeout", Duration.class);
            Object closeOptions = timeoutMethod.invoke(null, timeout);
            
            // .withGroupMembershipOperation(GroupMembershipOperation.REMAIN_IN_GROUP) fluent API 사용
            Method withGroupMembershipOperation = closeOptionsClass.getMethod(
                "withGroupMembershipOperation", groupMembershipOperationEnum);
            closeOptions = withGroupMembershipOperation.invoke(closeOptions, remainInGroup);
            
            // consumer.close(CloseOptions) 호출
            Method closeMethod = consumer.getClass().getMethod("close", closeOptionsClass);
            closeMethod.invoke(consumer, closeOptions);
            
            log.info("✅ Consumer.close(CloseOptions) 호출 완료");
            log.info("   - GroupMembershipOperation: REMAIN_IN_GROUP");
            log.info("   - Timeout: {}초", timeout.getSeconds());
            
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

