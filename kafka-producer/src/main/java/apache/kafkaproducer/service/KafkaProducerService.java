package apache.kafkaproducer.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class KafkaProducerService {

    // Spring Kafka가 제공하는 Kafka Producer 템플릿
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    // 메트릭 레지스트리
    private final MeterRegistry meterRegistry;
    
    // 프로듀서 메시지 카운터
    private Counter producerCounter;
    
    // 생성자에서 카운터 초기화
    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.producerCounter = createCounter();
    }
    
    /**
     * 새로운 카운터를 생성합니다.
     * @return 새로 생성된 카운터
     */
    private Counter createCounter() {
        return Counter.builder("producer_msg_total")
                .description("전체 프로듀스된 메시지 수")
                .register(meterRegistry);
    }

    /**
     * 지정된 토픽으로 메시지를 비동기적으로 보냅니다.
     * @param topic 보낼 토픽 이름
     * @param key 메시지 키
     * @param message 보낼 메시지 객체
     */
    public void sendMessage(String topic, String key, Object message) {
        // 메시지 전송 시 카운터 증가 (JMeter 테스트 1번 당 1회 카운트)
        producerCounter.increment();
        
        // 디버깅을 위한 로그 추가
        double currentCount = producerCounter.count();
        if (currentCount % 1000 == 0) { // 1000개마다 로그 출력
            log.info("현재 카운터 값: {}", currentCount);
        }
        
        // KafkaTemplate을 사용하여 메시지 전송. 비동기로 동작함
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, message);

        // 전송 성공/실패 시 로그를 남기기 위한 콜백
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.debug("메시지 전송 성공! [Topic: {}, Partition: {}, Offset: {}]",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("메시지 전송 실패! [Error: {}]", ex.getMessage());
            }
        });
    }
    
    /**
     * 카운터를 초기화합니다. (단위테스트용)
     * 기존 카운터를 제거하고 새로운 카운터를 생성합니다.
     */
    public void resetCounter() {
        // 초기화 전 현재 카운터 값 로깅
        double beforeCount = producerCounter.count();
        log.info("카운터 초기화 전 값: {}", beforeCount);
        
        // 기존 카운터를 레지스트리에서 제거
        meterRegistry.remove(producerCounter);
        // 새로운 카운터 생성
        this.producerCounter = createCounter();
        
        log.info("프로듀서 메시지 카운터가 초기화되었습니다. (이전 값: {}, 현재 값: {})", 
                beforeCount, producerCounter.count());
    }
    
    /**
     * 현재 카운터 값을 조회합니다.
     * @return 현재 카운터 값
     */
    public double getCurrentCount() {
        return producerCounter.count();
    }
}

