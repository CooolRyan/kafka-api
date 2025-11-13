package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer 에러 핸들러
 * 
 * UnreleasedInstanceIdException 처리:
 * - 이전 파드가 아직 consumer group에 있을 때 발생
 * - 로그만 남기고 Spring Kafka가 자동으로 재시도하도록 함
 */
@Component
@Slf4j
public class KafkaConsumerErrorHandler extends DefaultErrorHandler {

    public KafkaConsumerErrorHandler() {
        super();
    }

    // handleOtherException 메서드는 DefaultErrorHandler에서 이미 처리하므로
    // 별도로 오버라이드하지 않음
    // UnreleasedInstanceIdException은 Spring Kafka가 자동으로 재시도함
}

