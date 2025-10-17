package apache.kafkaproducer.controller;

import apache.kafkaproducer.dto.MessageRequestDto;
import apache.kafkaproducer.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaProducerService producerService;

    /**
     * 지정된 토픽으로 메시지를 발행하는 API 엔드포인트
     * @param topic 메시지를 보낼 토픽
     * @param requestDto 메시지 Key와 Value를 담은 DTO
     * @return 처리 결과
     */
    @PostMapping("/produce/{topic}")
    public ResponseEntity<String> produceMessage(
            @PathVariable String topic,
            @RequestBody MessageRequestDto requestDto) {
        try {
            producerService.sendMessage(topic, requestDto.getKey(), requestDto.getMessage());
            return ResponseEntity.ok("메시지 발행 요청이 성공적으로 처리되었습니다.");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("메시지 발행 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * JMeter 테스트용 간단한 메시지 발행 엔드포인트
     * @param topic 메시지를 보낼 토픽
     * @return 처리 결과
     */
    @PostMapping("/test/{topic}")
    public ResponseEntity<String> testProduceMessage(@PathVariable String topic) {
        try {
            // 테스트용 메시지 생성
            String testKey = "test-key-" + System.currentTimeMillis();
            String testMessage = "Test message at " + System.currentTimeMillis();
            
            producerService.sendMessage(topic, testKey, testMessage);
            return ResponseEntity.ok("테스트 메시지 발행 완료: " + testKey);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("테스트 메시지 발행 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 프로듀서 메시지 카운터를 초기화합니다. (단위테스트용)
     * @return 처리 결과
     */
    @PostMapping("/reset-counter")
    public ResponseEntity<String> resetCounter() {
        try {
            producerService.resetCounter();
            return ResponseEntity.ok("카운터가 성공적으로 초기화되었습니다.");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("카운터 초기화 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 현재 프로듀서 메시지 카운터 값을 조회합니다.
     * @return 현재 카운터 값
     */
    @GetMapping("/counter")
    public ResponseEntity<Object> getCounter() {
        try {
            double count = producerService.getCurrentCount();
            return ResponseEntity.ok().body(new CounterResponse(count));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("카운터 조회 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 카운터 응답을 위한 DTO 클래스
     */
    public static class CounterResponse {
        private final double count;
        private final String timestamp;

        public CounterResponse(double count) {
            this.count = count;
            this.timestamp = java.time.LocalDateTime.now().toString();
        }

        public double getCount() {
            return count;
        }

        public String getTimestamp() {
            return timestamp;
        }
    }
}

