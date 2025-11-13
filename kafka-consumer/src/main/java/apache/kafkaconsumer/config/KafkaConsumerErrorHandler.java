package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
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
public class KafkaConsumerErrorHandler implements CommonErrorHandler {

    @Override
    public void handleOtherException(Exception thrownException, 
                                     boolean committed,
                                     MessageListenerContainer container) {
        
        Throwable cause = thrownException.getCause();
        if (cause instanceof UnreleasedInstanceIdException) {
            UnreleasedInstanceIdException exception = (UnreleasedInstanceIdException) cause;
            
            log.warn("⚠️ UnreleasedInstanceIdException 발생: {}", exception.getMessage());
            log.warn("   이전 파드가 아직 consumer group에 있습니다.");
            log.warn("   해결 방법:");
            log.warn("   1. 이전 파드가 완전히 종료될 때까지 대기 (권장)");
            log.warn("   2. 또는 consumer group을 수동으로 정리");
            log.warn("   3. 또는 group.instance.id를 고유하게 변경");
            log.warn("   Spring Kafka가 자동으로 재시도합니다...");
            
        } else {
            log.error("❌ Consumer 에러 발생: {}", thrownException.getMessage(), thrownException);
        }
    }
}

