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
}
