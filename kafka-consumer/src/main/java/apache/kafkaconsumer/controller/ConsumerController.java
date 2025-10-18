package apache.kafkaconsumer.controller;

import apache.kafkaconsumer.service.KafkaConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/consumer")
@RequiredArgsConstructor
public class ConsumerController {

    private final KafkaConsumerService consumerService;

    /**
     * Consumer 처리 상태 조회
     */
    @GetMapping("/status")
    public ResponseEntity<String> getStatus() {
        try {
            String status = consumerService.getStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("상태 조회 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * Health Check
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Consumer is running");
    }

    /**
     * DLQ 메시지 재처리 (수동)
     */
    @PostMapping("/dlq/retry")
    public ResponseEntity<String> retryDLQMessages() {
        try {
            // DLQ 토픽에서 메시지를 다시 원본 토픽으로 전송하는 로직
            // 실제 구현에서는 DLQ Consumer를 별도로 만들어야 함
            return ResponseEntity.ok("DLQ 재처리 요청이 완료되었습니다. (구현 필요)");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("DLQ 재처리 중 오류 발생: " + e.getMessage());
        }
    }
}
