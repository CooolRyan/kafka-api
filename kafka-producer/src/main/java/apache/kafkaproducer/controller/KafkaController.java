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
}
