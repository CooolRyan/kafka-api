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
            // 실제 Kafka Consumer 가져오기 (Spring Kafka 래퍼가 아닌)
            Consumer<?, ?> actualConsumer = getActualKafkaConsumer(consumer);
            if (actualConsumer == null) {
                log.warn("⚠️ 실제 Kafka Consumer를 찾을 수 없습니다. consumer.close() 사용");
                consumer.close(timeout);
                return;
            }
            
            // 1. CloseOptions 클래스 찾기 - Consumer의 close 메서드에서 동적으로 찾기
            log.info("🔍 CloseOptions 클래스 찾는 중...");
            Class<?> closeOptionsClass = null;
            
            // Consumer의 모든 close 메서드 확인
            Method[] methods = actualConsumer.getClass().getMethods();
            for (Method m : methods) {
                if (m.getName().equals("close") && m.getParameterCount() == 1) {
                    Class<?>[] paramTypes = m.getParameterTypes();
                    if (paramTypes.length == 1 && !paramTypes[0].equals(Duration.class)) {
                        // CloseOptions를 파라미터로 받는 close 메서드 찾음
                        closeOptionsClass = paramTypes[0];
                        log.info("✅ Consumer.close() 메서드에서 CloseOptions 클래스 발견: {}", closeOptionsClass.getName());
                        break;
                    }
                }
            }
            
            // 동적으로 찾지 못한 경우 여러 경로 시도
            if (closeOptionsClass == null) {
                String[] possiblePaths = {
                    "org.apache.kafka.clients.consumer.CloseOptions",
                    "org.apache.kafka.clients.consumer.Consumer$CloseOptions"
                };
                
                for (String path : possiblePaths) {
                    try {
                        closeOptionsClass = Class.forName(path);
                        log.info("✅ CloseOptions 클래스 찾음: {}", closeOptionsClass.getName());
                        break;
                    } catch (ClassNotFoundException e) {
                        log.debug("경로 '{}'에서 CloseOptions를 찾을 수 없음", path);
                    }
                }
            }
            
            if (closeOptionsClass == null) {
                throw new ClassNotFoundException("CloseOptions 클래스를 찾을 수 없습니다");
            }
            
            // 2. GroupMembershipOperation Enum 찾기
            log.info("🔍 GroupMembershipOperation Enum 찾는 중...");
            Class<?> groupMembershipOperationEnum = null;
            
            // CloseOptions 클래스의 내부 클래스로 먼저 시도
            Class<?>[] innerClasses = closeOptionsClass.getDeclaredClasses();
            for (Class<?> innerClass : innerClasses) {
                if (innerClass.getSimpleName().equals("GroupMembershipOperation")) {
                    groupMembershipOperationEnum = innerClass;
                    log.info("✅ CloseOptions 내부 클래스에서 GroupMembershipOperation 찾음: {}", groupMembershipOperationEnum.getName());
                    break;
                }
            }
            
            // 내부 클래스에서 찾지 못한 경우 여러 경로 시도
            if (groupMembershipOperationEnum == null) {
                String closeOptionsPackage = closeOptionsClass.getPackage().getName();
                String[] possiblePaths = {
                    closeOptionsClass.getName() + "$GroupMembershipOperation",
                    closeOptionsPackage + ".GroupMembershipOperation",
                    "org.apache.kafka.clients.consumer.GroupMembershipOperation"
                };
                
                for (String path : possiblePaths) {
                    try {
                        groupMembershipOperationEnum = Class.forName(path);
                        log.info("✅ GroupMembershipOperation Enum 찾음: {}", groupMembershipOperationEnum.getName());
                        break;
                    } catch (ClassNotFoundException e) {
                        log.debug("경로 '{}'에서 GroupMembershipOperation를 찾을 수 없음", path);
                    }
                }
            }
            
            if (groupMembershipOperationEnum == null) {
                throw new ClassNotFoundException("GroupMembershipOperation Enum을 찾을 수 없습니다");
            }
            
            // Enum 값들 확인
            Object[] enumValues = groupMembershipOperationEnum.getEnumConstants();
            log.info("🔍 GroupMembershipOperation Enum 값들:");
            for (Object enumValue : enumValues) {
                log.info("   - {}", enumValue);
            }
            
            // DONT_LEAVE_GROUP 또는 REMAIN_IN_GROUP 찾기
            Object groupMembershipOp = null;
            String[] possibleNames = {"DONT_LEAVE_GROUP", "REMAIN_IN_GROUP", "LEAVE_GROUP"};
            
            for (String name : possibleNames) {
                try {
                    groupMembershipOp = Enum.valueOf((Class<Enum>) groupMembershipOperationEnum, name);
                    log.info("✅ {} Enum 값 찾음: {}", name, groupMembershipOp);
                    break;
                } catch (IllegalArgumentException e) {
                    log.debug("Enum 값 '{}'을 찾을 수 없음", name);
                }
            }
            
            if (groupMembershipOp == null) {
                // 첫 번째 Enum 값 사용 (fallback)
                if (enumValues.length > 0) {
                    groupMembershipOp = enumValues[0];
                    log.warn("⚠️ 기본 Enum 값 사용: {}", groupMembershipOp);
                } else {
                    throw new IllegalArgumentException("GroupMembershipOperation Enum 값을 찾을 수 없습니다");
                }
            }
            
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
            closeOptions = groupMembershipMethod.invoke(closeOptions, groupMembershipOp);
            log.info("✅ CloseOptions에 GroupMembershipOperation 설정 완료: {}", groupMembershipOp);
            
            // 6. Consumer.close(CloseOptions) 메서드 찾기 (실제 Kafka Consumer 사용)
            log.info("🔍 Consumer.close(CloseOptions) 메서드 찾는 중...");
            Method closeMethod = actualConsumer.getClass().getMethod("close", closeOptionsClass);
            log.info("✅ close(CloseOptions) 메서드 찾음: {}", closeMethod);
            
            // 7. close() 호출
            log.info("🚀 Consumer.close(CloseOptions) 호출 시작...");
            log.info("   - Timeout: {}초", timeout.getSeconds());
            log.info("   - GroupMembershipOperation: {}", groupMembershipOp);
            closeMethod.invoke(actualConsumer, closeOptions);
            
            log.info("✅ Consumer.close(CloseOptions) 호출 완료");
            
        } catch (ClassNotFoundException e) {
            log.error("❌❌❌ CloseOptions 클래스를 찾을 수 없습니다! ❌❌❌");
            log.error("   - 찾은 경로: org.apache.kafka.clients.consumer.CloseOptions");
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
    
    /**
     * Spring Kafka 래퍼에서 실제 Kafka Consumer 추출
     */
    private Consumer<?, ?> getActualKafkaConsumer(Consumer<?, ?> consumer) {
        try {
            // Spring Kafka의 ExtendedKafkaConsumer인 경우 실제 consumer 필드 추출
            if (consumer.getClass().getName().contains("ExtendedKafkaConsumer")) {
                Field delegateField = consumer.getClass().getDeclaredField("delegate");
                delegateField.setAccessible(true);
                Object delegate = delegateField.get(consumer);
                if (delegate instanceof Consumer) {
                    return (Consumer<?, ?>) delegate;
                }
            }
            // 이미 실제 Consumer인 경우
            if (consumer.getClass().getName().equals("org.apache.kafka.clients.consumer.KafkaConsumer")) {
                return consumer;
            }
        } catch (Exception e) {
            log.debug("실제 Kafka Consumer 추출 실패: {}", e.getMessage());
        }
        return consumer; // fallback
    }
}

