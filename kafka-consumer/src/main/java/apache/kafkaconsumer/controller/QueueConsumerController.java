package apache.kafkaconsumer.controller;

import apache.kafkaconsumer.service.QueueKafkaConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/queue-consumer")
@RequiredArgsConstructor
public class QueueConsumerController {

    private final QueueKafkaConsumerService queueConsumerService;

    /**
     * Queue Consumer 처리 상태 조회
     */
    @GetMapping("/status")
    public ResponseEntity<String> getQueueStatus() {
        try {
            String status = queueConsumerService.getQueueStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Queue 상태 조회 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * Queue Consumer Health Check
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Queue Consumer is running");
    }

    /**
     * Queue DLQ 메시지 재처리 (수동)
     */
    @PostMapping("/dlq/retry")
    public ResponseEntity<String> retryQueueDLQMessages() {
        try {
            return ResponseEntity.ok("Queue DLQ 재처리 요청이 완료되었습니다. (구현 필요)");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Queue DLQ 재처리 중 오류 발생: " + e.getMessage());
        }
    }
}