package apache.kafkaconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    /**
     * 'my-test-topic' 토픽의 메시지를 리스닝합니다.
     * @param record 소비된 메시지의 전체 정보 (헤더, 키, 값 등)
     */
    @KafkaListener(topics = "my-test-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("메시지 수신! [Key: {}, Value: {}, Partition: {}, Offset: {}]",
                record.key(), record.value(), record.partition(), record.offset());
        // 여기에 메시지 수신 후 처리할 비즈니스 로직을 추가할 수 있습니다.
    }
}

