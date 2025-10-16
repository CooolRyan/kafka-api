// kafka-consumer/src/main/java/apache/kafkaconsumer/service/KafkaConsumerService.java
package apache.kafkaconsumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "jmeter", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        String value = record.value();

        // JSON 파싱 시도
        try {
            JsonNode jsonNode = objectMapper.readTree(value);
            log.info("JSON 메시지 수신! [Key: {}, Value: {}, Partition: {}, Offset: {}]",
                    record.key(), jsonNode, record.partition(), record.offset());
        } catch (Exception e) {
            // JSON이 아니면 String으로 처리
            log.info("String 메시지 수신! [Key: {}, Value: {}, Partition: {}, Offset: {}]",
                    record.key(), value, record.partition(), record.offset());
        }
    }
}