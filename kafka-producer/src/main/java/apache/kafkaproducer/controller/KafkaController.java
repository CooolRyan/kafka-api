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
            String podName = System.getenv("HOSTNAME");
            return ResponseEntity.ok("카운터가 성공적으로 초기화되었습니다. (Pod: " + podName + ")");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("카운터 초기화 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 모든 파드의 카운터를 초기화합니다. (MetalLB 로드밸런싱 테스트용)
     * @return 처리 결과
     */
    @PostMapping("/reset-counter/all")
    public ResponseEntity<String> resetAllCounters() {
        try {
            producerService.resetCounter();
            String podName = System.getenv("HOSTNAME");
            return ResponseEntity.ok("모든 파드의 카운터 초기화 요청이 완료되었습니다. (현재 Pod: " + podName + ")");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("전체 카운터 초기화 중 오류 발생: " + e.getMessage());
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
            String podName = System.getenv("HOSTNAME");
            return ResponseEntity.ok().body(new CounterResponse(count, podName));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("카운터 조회 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 모든 파드의 카운터를 합산하여 조회합니다. (MetalLB 로드밸런싱 테스트용)
     * @return 전체 카운터 값
     */
    @GetMapping("/counter/total")
    public ResponseEntity<Object> getTotalCounter() {
        try {
            // 현재 파드의 카운터
            double currentCount = producerService.getCurrentCount();
            String currentPodName = System.getenv("HOSTNAME");
            
            // 전체 카운터 정보 (현재는 현재 파드만, 추후 확장 가능)
            TotalCounterResponse response = new TotalCounterResponse();
            response.addPodCounter(currentPodName, currentCount);
            response.calculateTotal();
            
            return ResponseEntity.ok().body(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("전체 카운터 조회 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 카운터 응답을 위한 DTO 클래스
     */
    public static class CounterResponse {
        private final double count;
        private final String timestamp;
        private final String podName;

        public CounterResponse(double count) {
            this.count = count;
            this.timestamp = java.time.LocalDateTime.now().toString();
            this.podName = System.getenv("HOSTNAME");
        }

        public CounterResponse(double count, String podName) {
            this.count = count;
            this.timestamp = java.time.LocalDateTime.now().toString();
            this.podName = podName;
        }

        public double getCount() {
            return count;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public String getPodName() {
            return podName;
        }
    }

    /**
     * 전체 카운터 응답을 위한 DTO 클래스
     */
    public static class TotalCounterResponse {
        private final java.util.Map<String, Double> podCounters = new java.util.HashMap<>();
        private double totalCount = 0.0;
        private final String timestamp = java.time.LocalDateTime.now().toString();

        public void addPodCounter(String podName, double count) {
            podCounters.put(podName, count);
        }

        public void calculateTotal() {
            totalCount = podCounters.values().stream()
                    .mapToDouble(Double::doubleValue)
                    .sum();
        }

        public java.util.Map<String, Double> getPodCounters() {
            return podCounters;
        }

        public double getTotalCount() {
            return totalCount;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public int getPodCount() {
            return podCounters.size();
        }
    }
}

