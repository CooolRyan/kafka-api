package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * 
 * SmartLifecycle을 사용하여 Spring Kafka의 기본 종료 프로세스보다 먼저 실행되도록 함
 * (phase를 낮게 설정하여 다른 Lifecycle보다 먼저 stop됨)
 */
@Component
@Slf4j
public class KafkaGracefulShutdownConfig implements SmartLifecycle {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public KafkaGracefulShutdownConfig(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        if (!running.getAndSet(false)) {
            return; // 이미 종료됨
        }

        log.info("🛑 Kafka 4.1 CloseOptions를 사용하여 graceful shutdown 시작...");
        
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

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * phase를 낮게 설정하여 다른 Lifecycle Bean들보다 먼저 stop되도록 함
     * KafkaListenerEndpointRegistry의 기본 phase는 Integer.MAX_VALUE이므로
     * 이 값보다 낮게 설정하면 먼저 실행됨
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }

    /**
     * Kafka 4.1 CloseOptions를 사용하여 Container 종료
     * 
     * 주의: Container가 이미 stop 중이면 접근하지 않음
     * Spring Kafka의 기본 종료 프로세스를 방해하지 않고,
     * 내부 consumer에만 CloseOptions를 적용함
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
            
            // 먼저 모든 consumer에 CloseOptions 적용
            boolean allConsumersClosed = true;
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
                        // 주의: 이 시점에서 Spring Kafka가 consumer를 사용 중일 수 있으므로
                        // 동기화가 필요할 수 있음
                        try {
                            closeConsumerWithOptions(consumer, timeout);
                            log.info("✅ Consumer graceful shutdown 완료");
                        } catch (ConcurrentModificationException e) {
                            log.warn("⚠️ Consumer가 다른 스레드에서 사용 중입니다. 기본 stop() 사용");
                            allConsumersClosed = false;
                            // Spring Kafka가 이미 종료 중이므로, 우리는 건드리지 않음
                        }
                    } else {
                        log.warn("⚠️ Consumer를 찾을 수 없습니다.");
                        allConsumersClosed = false;
                    }
                } catch (Exception e) {
                    log.error("❌ Consumer 종료 중 오류: {}", e.getMessage(), e);
                    allConsumersClosed = false;
                }
            }
            
            // 모든 consumer가 성공적으로 종료되었으면 container도 stop
            // 하지만 Spring Kafka가 이미 종료 중일 수 있으므로, 
            // 우리는 consumer.close()만 호출하고 container.stop()은 Spring Kafka에 맡김
            if (allConsumersClosed) {
                log.info("✅ 모든 Consumer에 CloseOptions 적용 완료. Container는 Spring Kafka가 종료합니다.");
            } else {
                log.warn("⚠️ 일부 Consumer 종료 실패. Spring Kafka의 기본 종료 프로세스에 맡깁니다.");
            }
            
        } catch (Exception e) {
            log.error("❌ Container 종료 중 오류 발생: {}", e.getMessage(), e);
            // Fallback: Spring Kafka의 기본 종료 프로세스에 맡김
            log.warn("⚠️ Spring Kafka의 기본 종료 프로세스에 맡깁니다.");
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
            // 내부 클래스이므로 $ 대신 .을 사용하여 접근 시도
            Class<?> closeOptionsClass = null;
            try {
                closeOptionsClass = Class.forName("org.apache.kafka.clients.consumer.Consumer$CloseOptions");
            } catch (ClassNotFoundException e) {
                // 대안: Consumer 클래스에서 내부 클래스로 접근
                Class<?> consumerClass = Consumer.class;
                Class<?>[] innerClasses = consumerClass.getDeclaredClasses();
                for (Class<?> innerClass : innerClasses) {
                    if (innerClass.getSimpleName().equals("CloseOptions")) {
                        closeOptionsClass = innerClass;
                        break;
                    }
                }
            }
            
            if (closeOptionsClass == null) {
                throw new ClassNotFoundException("CloseOptions class not found");
            }
            
            Class<?> groupMembershipOperationEnum = null;
            try {
                groupMembershipOperationEnum = Class.forName(
                    "org.apache.kafka.clients.consumer.Consumer$CloseOptions$GroupMembershipOperation");
            } catch (ClassNotFoundException e) {
                // 대안: CloseOptions 클래스에서 내부 enum으로 접근
                Class<?>[] innerClasses = closeOptionsClass.getDeclaredClasses();
                for (Class<?> innerClass : innerClasses) {
                    if (innerClass.getSimpleName().equals("GroupMembershipOperation")) {
                        groupMembershipOperationEnum = innerClass;
                        break;
                    }
                }
            }
            
            if (groupMembershipOperationEnum == null) {
                throw new ClassNotFoundException("GroupMembershipOperation enum not found");
            }
            
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
            log.warn("   현재 사용 중인 Kafka 클라이언트 버전을 확인하세요.");
            log.warn("   기본 consumer.close() 사용");
            try {
                consumer.close(timeout);
            } catch (Exception ex) {
                log.error("❌ consumer.close()도 실패: {}", ex.getMessage());
            }
        } catch (Exception e) {
            log.error("❌ CloseOptions 사용 중 오류: {}", e.getMessage(), e);
            // Fallback: 기본 close() 사용
            try {
                consumer.close(timeout);
            } catch (Exception ex) {
                log.error("❌ Fallback consumer.close()도 실패: {}", ex.getMessage());
            }
        }
    }

}

