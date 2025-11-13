# Kafka 4.1 Graceful Shutdown 내부 로직

## 전체 흐름

```
1. Kubernetes → SIGTERM 전송
   ↓
2. Spring Context → ContextClosedEvent 발생
   ↓
3. KafkaGracefulShutdownConfig.onApplicationEvent() 실행
   ↓
4. KafkaListenerEndpointRegistry에서 모든 Container 가져오기
   ↓
5. 각 Container의 내부 Consumer에 접근 (리플렉션)
   ↓
6. Consumer.close(CloseOptions) 호출
   ↓
7. Kafka 4.1이 REMAIN_IN_GROUP 옵션 처리
   ↓
8. 60초 동안 consumer group에 머물러 있음
   ↓
9. 새 파드가 준비되어 consumer group에 조인
   ↓
10. 기존 파드 안전하게 종료
```

## 상세 단계별 설명

### 1단계: 이벤트 수신
```java
@Override
public void onApplicationEvent(ContextClosedEvent event) {
    // Spring Context가 종료될 때 자동으로 호출됨
    // Kubernetes가 SIGTERM을 보내면 Spring이 이 이벤트를 발생시킴
}
```

### 2단계: Container 찾기
```java
Collection<MessageListenerContainer> containers = 
    kafkaListenerEndpointRegistry.getAllListenerContainers();
// @KafkaListener로 등록된 모든 listener container를 가져옴
```

### 3단계: 내부 Consumer 접근 (리플렉션)

#### 3-1. ConcurrentMessageListenerContainer 구조
```
ConcurrentMessageListenerContainer
  └── containers (Collection<KafkaMessageListenerContainer>)
       └── KafkaMessageListenerContainer
            └── listenerConsumer (ListenerConsumer)
                 └── consumer (KafkaConsumer)
```

#### 3-2. 리플렉션으로 접근
```java
// 1. ConcurrentMessageListenerContainer의 containers 필드 접근
Field containersField = ConcurrentMessageListenerContainer.class
    .getDeclaredField("containers");
containersField.setAccessible(true);
Collection<KafkaMessageListenerContainer> containers = 
    (Collection) containersField.get(container);

// 2. KafkaMessageListenerContainer의 listenerConsumer 필드 접근
Field listenerConsumerField = KafkaMessageListenerContainer.class
    .getDeclaredField("listenerConsumer");
listenerConsumerField.setAccessible(true);
Object listenerConsumer = listenerConsumerField.get(container);

// 3. ListenerConsumer의 consumer 필드 접근
Field consumerField = listenerConsumer.getClass()
    .getDeclaredField("consumer");
consumerField.setAccessible(true);
Consumer consumer = (Consumer) consumerField.get(listenerConsumer);
```

### 4단계: CloseOptions 생성 및 적용

#### 4-1. CloseOptions 클래스 로드
```java
Class<?> closeOptionsClass = Class.forName(
    "org.apache.kafka.clients.consumer.Consumer$CloseOptions");
```

#### 4-2. GroupMembershipOperation Enum 로드
```java
Class<?> groupMembershipOperationEnum = Class.forName(
    "org.apache.kafka.clients.consumer.Consumer$CloseOptions$GroupMembershipOperation");
Object remainInGroup = Enum.valueOf(
    (Class<Enum>) groupMembershipOperationEnum, "REMAIN_IN_GROUP");
```

#### 4-3. CloseOptions 인스턴스 생성 (Static Factory Method)
```java
// CloseOptions.timeout(Duration.ofSeconds(60)) 호출
Method timeoutMethod = closeOptionsClass.getMethod("timeout", Duration.class);
Object closeOptions = timeoutMethod.invoke(null, Duration.ofSeconds(60));
```

#### 4-4. Fluent API로 옵션 추가
```java
// .withGroupMembershipOperation(REMAIN_IN_GROUP) 호출
Method withGroupMembershipOperation = closeOptionsClass.getMethod(
    "withGroupMembershipOperation", groupMembershipOperationEnum);
closeOptions = withGroupMembershipOperation.invoke(closeOptions, remainInGroup);
```

### 5단계: Consumer.close(CloseOptions) 호출
```java
Method closeMethod = consumer.getClass().getMethod("close", closeOptionsClass);
closeMethod.invoke(consumer, closeOptions);
```

### 6단계: Kafka 4.1 내부 처리

Kafka 4.1의 `Consumer.close(CloseOptions)` 내부 동작:

1. **REMAIN_IN_GROUP 옵션 확인**
   - `GroupMembershipOperation.REMAIN_IN_GROUP`이면 consumer group에서 leave하지 않음
   - `GroupMembershipOperation.LEAVE_GROUP`이면 즉시 leave group 요청 전송

2. **Timeout 처리**
   - 지정된 timeout(60초) 동안 consumer가 consumer group에 머물러 있음
   - 이 시간 동안 하트비트는 계속 전송되어 consumer group에서 제거되지 않음

3. **Graceful Shutdown**
   - 진행 중인 poll() 작업 완료 대기
   - 처리 중인 메시지 커밋
   - timeout이 지나면 consumer 종료

## 왜 리플렉션을 사용하는가?

Spring Kafka의 내부 구조상:
- `KafkaMessageListenerContainer`의 `consumer` 필드는 `private` 또는 `protected`
- `ListenerConsumer`도 내부 클래스로 직접 접근 불가
- 따라서 리플렉션을 사용하여 내부 consumer에 접근해야 함

## 대안 방법

만약 Spring Kafka가 Kafka 4.1을 공식 지원한다면:
- `KafkaMessageListenerContainer`에 `close(CloseOptions)` 메서드가 추가될 수 있음
- 그 경우 리플렉션 없이 직접 호출 가능

## 주의사항

1. **Kafka 4.1+ 버전 필요**: CloseOptions는 Kafka 4.1부터 사용 가능
2. **리플렉션 오버헤드**: 런타임에 클래스를 로드하므로 약간의 오버헤드 있음
3. **Fallback 처리**: CloseOptions를 찾을 수 없으면 기본 `consumer.close(timeout)` 사용



