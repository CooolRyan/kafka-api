package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Kafka Consumer 에러 핸들러
 * 
 * UnreleasedInstanceIdException 처리:
 * - 이전 파드가 아직 consumer group에 있을 때 발생
 * - 로그만 남기고 Spring Kafka가 자동으로 재시도하도록 함
 */
@Component
@Slf4j
public class KafkaConsumerErrorHandler implements CommonErrorHandler {

    public void handleOtherException(Exception thrownException, 
                                     boolean committed,
                                     Consumer<?, ?> consumer,
                                     MessageListenerContainer container) {
        
        Throwable cause = thrownException.getCause();
        if (cause instanceof UnreleasedInstanceIdException) {
            UnreleasedInstanceIdException exception = (UnreleasedInstanceIdException) cause;
            
            log.warn("⚠️ UnreleasedInstanceIdException 발생: {}", exception.getMessage());
            log.warn("   이전 파드가 아직 consumer group에 있습니다.");
            log.warn("   Spring Kafka가 자동으로 재시도합니다...");
            
        } else {
            log.error("❌ Consumer 에러 발생: {}", thrownException.getMessage(), thrownException);
        }
    }

    public void handleRemaining(Exception thrownException, 
                                List<ConsumerRecord<?, ?>> records,
                                Consumer<?, ?> consumer,
                                MessageListenerContainer container) {
        log.error("❌ 처리되지 않은 레코드가 있습니다: {}개", records.size());
        log.error("   에러: {}", thrownException.getMessage(), thrownException);
    }
}

