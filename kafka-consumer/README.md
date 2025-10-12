# Kafka Consumer Service

이 프로젝트는 Kafka 메시지를 소비하는 독립적인 Spring Boot 서비스입니다.

## 기능

- Kafka 토픽에서 메시지 수신
- 자동 오프셋 관리
- 메시지 처리 로그

## 설정

- 서버 포트: 8081
- Consumer Group: kafka-consumer-group
- 수신 토픽: my-test-topic
- Kafka 서버: kafka1:9092,kafka2:9092,kafka3:9092

## 실행 방법

### 로컬 실행
```bash
./gradlew bootRun
```

### Docker 실행
```bash
docker build -t kafka-consumer .
docker run -p 8081:8081 kafka-consumer
```

## 메시지 처리

Consumer는 `my-test-topic` 토픽의 메시지를 수신하여 로그로 출력합니다.
수신된 메시지의 키, 값, 파티션, 오프셋 정보가 로그에 기록됩니다.





