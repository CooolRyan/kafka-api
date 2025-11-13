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
import java.util.ConcurrentModificationException;

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
            // Container가 이미 종료 중이면 skip
            if (!container.isRunning()) {
                log.warn("⚠️ Container가 이미 종료 중입니다. skip");
                return;
            }
            
            Field containersField = ConcurrentMessageListenerContainer.class.getDeclaredField("containers");
            containersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Collection<KafkaMessageListenerContainer<?, ?>> containers = 
                (Collection<KafkaMessageListenerContainer<?, ?>>) containersField.get(container);
            
            for (KafkaMessageListenerContainer<?, ?> kafkaContainer : containers) {
                if (!kafkaContainer.isRunning()) {
                    continue;
                }
                
                // Consumer에 접근하기 전에 container가 여전히 실행 중인지 확인
                if (!kafkaContainer.isRunning()) {
                    continue;
                }
                
                Consumer<?, ?> consumer = getConsumerFromContainer(kafkaContainer);
                
                if (consumer != null) {
                    // CloseOptions 사용 시도
                    try {
                        closeConsumerWithOptions(consumer, timeout);
                        log.info("✅ Consumer.close(CloseOptions) 호출 완료");
                    } catch (java.util.ConcurrentModificationException e) {
                        log.warn("⚠️ Consumer가 다른 스레드에서 사용 중입니다. container.stop() 사용");
                        // Spring Kafka가 이미 종료 중이므로 container.stop()만 호출
                        kafkaContainer.stop();
                    }
                } else {
                    kafkaContainer.stop();
                }
            }
            
        } catch (Exception e) {
            log.error("❌ Container 종료 중 오류: {}", e.getMessage(), e);
            // Fallback: container.stop()만 호출
            try {
                container.stop();
            } catch (Exception ex) {
                log.error("❌ container.stop()도 실패: {}", ex.getMessage());
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private Consumer<?, ?> getConsumerFromContainer(KafkaMessageListenerContainer<?, ?> container) {
        try {
            // Container가 실행 중일 때만 접근
            if (!container.isRunning()) {
                return null;
            }
            
            Field listenerConsumerField = KafkaMessageListenerContainer.class.getDeclaredField("listenerConsumer");
            listenerConsumerField.setAccessible(true);
            Object listenerConsumer = listenerConsumerField.get(container);
            
            if (listenerConsumer != null) {
                Field consumerField = listenerConsumer.getClass().getDeclaredField("consumer");
                consumerField.setAccessible(true);
                Consumer<?, ?> consumer = (Consumer<?, ?>) consumerField.get(listenerConsumer);
                
                // Consumer가 null이 아니고 container가 여전히 실행 중인지 확인
                if (consumer != null && container.isRunning()) {
                    return consumer;
                }
            }
        } catch (Exception e) {
            log.debug("리플렉션으로 consumer 접근 실패: {}", e.getMessage());
        }
        return null;
    }
    
    private void closeConsumerWithOptions(Consumer<?, ?> consumer, Duration timeout) {
        try {
            // 1. CloseOptions 클래스 찾기
            log.info("🔍 CloseOptions 클래스 찾는 중...");
            Class<?> closeOptionsClass = Class.forName("org.apache.kafka.clients.consumer.Consumer$CloseOptions");
            log.info("✅ CloseOptions 클래스 찾음: {}", closeOptionsClass.getName());
            
            // 2. GroupMembershipOperation Enum 찾기
            log.info("🔍 GroupMembershipOperation Enum 찾는 중...");
            Class<?> groupMembershipOperationEnum = Class.forName(
                "org.apache.kafka.clients.consumer.Consumer$CloseOptions$GroupMembershipOperation");
            log.info("✅ GroupMembershipOperation Enum 찾음: {}", groupMembershipOperationEnum.getName());
            
            // DONT_LEAVE_GROUP 사용 (REMAIN_IN_GROUP이 아님!)
            Object dontLeaveGroup = Enum.valueOf((Class<Enum>) groupMembershipOperationEnum, "DONT_LEAVE_GROUP");
            log.info("✅ DONT_LEAVE_GROUP Enum 값: {}", dontLeaveGroup);
            
            // 3. CloseOptions 생성자 찾기 (new CloseOptions())
            log.info("🔍 CloseOptions 생성자 찾는 중...");
            java.lang.reflect.Constructor<?> closeOptionsConstructor = closeOptionsClass.getDeclaredConstructor();
            closeOptionsConstructor.setAccessible(true);
            Object closeOptions = closeOptionsConstructor.newInstance();
            log.info("✅ CloseOptions 인스턴스 생성 완료");
            
            // 4. timeout(Duration) 메서드 찾기 (builder pattern)
            log.info("🔍 CloseOptions.timeout() 메서드 찾는 중...");
            Method timeoutMethod = closeOptionsClass.getMethod("timeout", Duration.class);
            log.info("✅ timeout() 메서드 찾음: {}", timeoutMethod);
            closeOptions = timeoutMethod.invoke(closeOptions, timeout);
            log.info("✅ CloseOptions에 timeout 설정 완료: {}초", timeout.getSeconds());
            
            // 5. groupMembership() 메서드 찾기 (builder pattern)
            log.info("🔍 CloseOptions.groupMembership() 메서드 찾는 중...");
            Method groupMembershipMethod = closeOptionsClass.getMethod("groupMembership", groupMembershipOperationEnum);
            log.info("✅ groupMembership() 메서드 찾음: {}", groupMembershipMethod);
            closeOptions = groupMembershipMethod.invoke(closeOptions, dontLeaveGroup);
            log.info("✅ CloseOptions에 DONT_LEAVE_GROUP 설정 완료");
            
            // 6. Consumer.close(CloseOptions) 메서드 찾기
            log.info("🔍 Consumer.close(CloseOptions) 메서드 찾는 중...");
            Method closeMethod = consumer.getClass().getMethod("close", closeOptionsClass);
            log.info("✅ close(CloseOptions) 메서드 찾음: {}", closeMethod);
            
            // 7. close() 호출
            log.info("🚀 Consumer.close(CloseOptions) 호출 시작...");
            log.info("   - Timeout: {}초", timeout.getSeconds());
            log.info("   - GroupMembershipOperation: DONT_LEAVE_GROUP");
            closeMethod.invoke(consumer, closeOptions);
            
            log.info("✅ Consumer.close(CloseOptions) 호출 완료");
            
        } catch (ClassNotFoundException e) {
            log.error("❌❌❌ CloseOptions 클래스를 찾을 수 없습니다! ❌❌❌");
            log.error("   - Kafka 버전 확인 필요: kafka-clients:4.1.0이 실제로 포함되었는지 확인");
            log.error("   - 의존성 트리 확인: gradle dependencies | grep kafka-clients");
            log.error("   - 예외: {}", e.getMessage(), e);
            try {
                log.warn("⚠️ 기본 consumer.close() 사용");
                consumer.close(timeout);
            } catch (Exception ex) {
                log.error("❌ consumer.close()도 실패: {}", ex.getMessage());
            }
        } catch (NoSuchMethodException e) {
            log.error("❌❌❌ 메서드를 찾을 수 없습니다! ❌❌❌");
            log.error("   - 찾지 못한 메서드: {}", e.getMessage());
            log.error("   - 예외: {}", e.getMessage(), e);
            try {
                log.warn("⚠️ 기본 consumer.close() 사용");
                consumer.close(timeout);
            } catch (Exception ex) {
                log.error("❌ consumer.close()도 실패: {}", ex.getMessage());
            }
        } catch (java.util.ConcurrentModificationException e) {
            log.warn("⚠️ Consumer가 다른 스레드에서 사용 중입니다. consumer.close() 호출 불가");
            throw e; // 상위로 전파하여 container.stop() 사용
        } catch (Exception e) {
            log.error("❌ CloseOptions 사용 중 오류: {}", e.getMessage(), e);
            log.error("   - 예외 타입: {}", e.getClass().getName());
            try {
                log.warn("⚠️ 기본 consumer.close() 사용");
                consumer.close(timeout);
            } catch (Exception ex) {
                log.error("❌ consumer.close()도 실패: {}", ex.getMessage());
            }
        }
    }
}

