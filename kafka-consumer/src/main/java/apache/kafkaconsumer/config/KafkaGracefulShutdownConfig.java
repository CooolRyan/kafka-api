package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.ConcurrentModificationException;

/**
 * Kafka 4.1 CloseOptions를 사용한 Graceful Shutdown
 * SmartLifecycle을 구현하여 Spring Kafka보다 먼저 실행되도록 함
 */
@Component
@Slf4j
public class KafkaGracefulShutdownConfig implements ApplicationListener<ContextClosedEvent>, SmartLifecycle {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private volatile boolean running = false;

    public KafkaGracefulShutdownConfig(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }
    
    // SmartLifecycle 구현
    @Override
    public void start() {
        running = true;
    }
    
    @Override
    public void stop() {
        running = false;
        gracefulShutdown();
    }
    
    @Override
    public boolean isRunning() {
        return running;
    }
    
    @Override
    public int getPhase() {
        // Spring Kafka의 기본 phase보다 낮게 설정하여 먼저 실행되도록 함
        // Spring Kafka의 기본 phase는 Integer.MAX_VALUE이므로, 그보다 낮은 값 사용
        return Integer.MAX_VALUE - 1000;
    }

    public void onApplicationEvent(ContextClosedEvent event) {
        // SmartLifecycle.stop()에서 처리하므로 여기서는 로그만
        log.debug("ContextClosedEvent 수신 - SmartLifecycle.stop()에서 처리됨");
    }
    
    private void gracefulShutdown() {
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
            
            // Container를 중지시키기 전에 consumer를 가져와서 닫기
            // Container가 중지되면 consumer에 접근할 수 없으므로, 먼저 가져와야 함
            log.info("🔄 Consumer를 가져와서 CloseOptions로 닫는 중...");
            
            Field containersField = ConcurrentMessageListenerContainer.class.getDeclaredField("containers");
            containersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Collection<KafkaMessageListenerContainer<?, ?>> containers = 
                (Collection<KafkaMessageListenerContainer<?, ?>>) containersField.get(container);
            
            // Consumer를 먼저 가져와서 닫기
            for (KafkaMessageListenerContainer<?, ?> kafkaContainer : containers) {
                if (!kafkaContainer.isRunning()) {
                    continue;
                }
                
                // Consumer에 접근 (container가 실행 중일 때만 가능)
                Consumer<?, ?> consumer = getConsumerFromContainer(kafkaContainer);
                
                if (consumer != null) {
                    // CloseOptions 사용 시도
                    try {
                        closeConsumerWithOptions(consumer, timeout);
                        log.info("✅ Consumer.close(CloseOptions) 호출 완료");
                        // Consumer를 닫은 후 container 중지
                        kafkaContainer.stop();
                    } catch (java.util.ConcurrentModificationException e) {
                        log.warn("⚠️ Consumer가 다른 스레드에서 사용 중입니다. container.stop() 사용");
                        // Spring Kafka가 이미 종료 중이므로 container.stop()만 호출
                        kafkaContainer.stop();
                    } catch (InvocationTargetException e) {
                        // InvocationTargetException의 원인 확인
                        Throwable cause = e.getCause();
                        if (cause instanceof java.util.ConcurrentModificationException) {
                            log.warn("⚠️ Consumer가 다른 스레드에서 사용 중입니다. container.stop() 사용");
                            kafkaContainer.stop();
                        } else {
                            log.error("❌ CloseOptions 호출 중 오류: {}", cause.getMessage(), cause);
                            kafkaContainer.stop();
                        }
                    }
                } else {
                    // Consumer를 가져올 수 없으면 container만 중지
                    log.warn("⚠️ Consumer를 가져올 수 없습니다. container.stop()만 호출");
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
            
            // 3. CloseOptions의 모든 메서드 확인
            log.info("🔍 CloseOptions의 모든 메서드 확인 중...");
            Method[] allMethods = closeOptionsClass.getMethods();
            for (Method m : allMethods) {
                log.info("   - {} (static: {})", m, java.lang.reflect.Modifier.isStatic(m.getModifiers()));
            }
            
            // 4. timeout(Duration) 메서드 찾기 (static일 수 있음)
            log.info("🔍 CloseOptions.timeout() 메서드 찾는 중...");
            Method timeoutMethod = null;
            try {
                // static 메서드로 시도
                timeoutMethod = closeOptionsClass.getMethod("timeout", Duration.class);
                log.info("✅ timeout() 메서드 찾음: {} (static: {})", timeoutMethod, 
                        java.lang.reflect.Modifier.isStatic(timeoutMethod.getModifiers()));
            } catch (NoSuchMethodException e) {
                log.error("❌ timeout() 메서드를 찾을 수 없습니다");
                throw e;
            }
            
            // timeout() 호출 (static이면 null, 아니면 인스턴스 필요)
            Object closeOptions;
            if (java.lang.reflect.Modifier.isStatic(timeoutMethod.getModifiers())) {
                closeOptions = timeoutMethod.invoke(null, timeout);
                log.info("✅ CloseOptions.timeout() static 호출 완료: {}초", timeout.getSeconds());
            } else {
                // 인스턴스 생성 후 호출
                java.lang.reflect.Constructor<?> closeOptionsConstructor = closeOptionsClass.getDeclaredConstructor();
                closeOptionsConstructor.setAccessible(true);
                closeOptions = closeOptionsConstructor.newInstance();
                log.info("✅ CloseOptions 인스턴스 생성 완료");
                closeOptions = timeoutMethod.invoke(closeOptions, timeout);
                log.info("✅ CloseOptions에 timeout 설정 완료: {}초", timeout.getSeconds());
            }
            
            // 5. groupMembership() 또는 다른 이름의 메서드 찾기
            log.info("🔍 GroupMembershipOperation 설정 메서드 찾는 중...");
            Method groupMembershipMethod = null;
            String[] possibleMethodNames = {"groupMembership", "withGroupMembership", "setGroupMembership"};
            
            for (String methodName : possibleMethodNames) {
                try {
                    groupMembershipMethod = closeOptionsClass.getMethod(methodName, groupMembershipOperationEnum);
                    log.info("✅ {}() 메서드 찾음: {}", methodName, groupMembershipMethod);
                    break;
                } catch (NoSuchMethodException e) {
                    log.debug("메서드 '{}'을 찾을 수 없음", methodName);
                }
            }
            
            // 메서드를 찾지 못한 경우, 파라미터 타입이 다른지 확인
            if (groupMembershipMethod == null) {
                log.info("🔍 GroupMembershipOperation을 받는 다른 메서드 찾는 중...");
                for (Method m : allMethods) {
                    Class<?>[] paramTypes = m.getParameterTypes();
                    if (paramTypes.length == 1 && paramTypes[0].equals(groupMembershipOperationEnum)) {
                        groupMembershipMethod = m;
                        log.info("✅ 메서드 발견: {}", m);
                        break;
                    }
                }
            }
            
            if (groupMembershipMethod != null) {
                if (java.lang.reflect.Modifier.isStatic(groupMembershipMethod.getModifiers())) {
                    closeOptions = groupMembershipMethod.invoke(null, groupMembershipOp);
                } else {
                    closeOptions = groupMembershipMethod.invoke(closeOptions, groupMembershipOp);
                }
                log.info("✅ CloseOptions에 GroupMembershipOperation 설정 완료: {}", groupMembershipOp);
            } else {
                log.warn("⚠️ GroupMembershipOperation 설정 메서드를 찾을 수 없습니다. timeout만 설정합니다.");
            }
            
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
        } catch (InvocationTargetException e) {
            // InvocationTargetException의 원인 확인
            Throwable cause = e.getCause();
            if (cause instanceof java.util.ConcurrentModificationException) {
                log.warn("⚠️ Consumer가 다른 스레드에서 사용 중입니다. consumer.close() 호출 불가");
                throw (java.util.ConcurrentModificationException) cause;
            }
            throw new RuntimeException("CloseOptions 호출 중 오류", e);
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

