package apache.kafkaproducer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    // Spring Kafka가 제공하는 Kafka Producer 템플릿
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 지정된 토픽으로 메시지를 비동기적으로 보냅니다.
     * @param topic 보낼 토픽 이름
     * @param key 메시지 키
     * @param message 보낼 메시지 객체
     */
    public void sendMessage(String topic, String key, Object message) {
        // KafkaTemplate을 사용하여 메시지 전송. 비동기로 동작함
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, message);

        // 전송 성공/실패 시 로그를 남기기 위한 콜백
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("메시지 전송 성공! [Topic: {}, Partition: {}, Offset: {}]",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("메시지 전송 실패! [Error: {}]", ex.getMessage());
            }
        });
    }
}
