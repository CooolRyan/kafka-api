# Kafka Producer Service

이 프로젝트는 Kafka 메시지를 발행하는 독립적인 Spring Boot 서비스입니다.

## 기능

- REST API를 통한 Kafka 메시지 발행
- 비동기 메시지 전송
- 전송 성공/실패 로그

## API 엔드포인트

### 메시지 발행
```
POST /api/kafka/produce/{topic}
Content-Type: application/json

{
    "key": "message-key",
    "message": "your-message-content"
}
```

## 실행 방법

### 로컬 실행
```bash
./gradlew bootRun
```

### Docker 실행
```bash
docker build -t kafka-producer .
docker run -p 8080:8080 kafka-producer
```

## 설정

- 서버 포트: 8080
- Kafka 서버: kafka1:9092,kafka2:9092,kafka3:9092










