// kafka-consumer/src/main/java/apache/kafkaconsumer/service/KafkaConsumerService.java
package apache.kafkaconsumer.service;

import apache.kafkaconsumer.entity.MessageEntity;
import apache.kafkaconsumer.repository.MessageRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class KafkaConsumerService {

    private final MessageRepository messageRepository;
    private final MeterRegistry meterRegistry;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final GCMonitoringService gcMonitoringService;
    
    // 배치 처리를 위한 큐
    private final BlockingQueue<MessageEntity> messageQueue = new LinkedBlockingQueue<>();
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong batchCount = new AtomicLong(0);
    
    // 오프셋 커밋을 위한 큐 (배치 처리 완료 후 커밋)
    private final BlockingQueue<Acknowledgment> acknowledgmentQueue = new LinkedBlockingQueue<>();
    
    // 메트릭 카운터
    private final Counter consumerCounter;
    private final Counter dbInsertCounter;
    private final Counter dlqCounter;
    
    public KafkaConsumerService(MessageRepository messageRepository, MeterRegistry meterRegistry, KafkaTemplate<String, Object> kafkaTemplate, GCMonitoringService gcMonitoringService) {
        this.messageRepository = messageRepository;
        this.meterRegistry = meterRegistry;
        this.kafkaTemplate = kafkaTemplate;
        this.gcMonitoringService = gcMonitoringService;
        this.consumerCounter = Counter.builder("consumer_msg_total")
                .description("전체 컨슈머된 메시지 수")
                .register(meterRegistry);
        this.dbInsertCounter = Counter.builder("db_insert_total")
                .description("전체 DB 삽입된 메시지 수")
                .register(meterRegistry);
        this.dlqCounter = Counter.builder("dlq_msg_total")
                .description("전체 DLQ로 전송된 메시지 수")
                .register(meterRegistry);
        
        // 배치 처리 스레드 시작
        startBatchProcessor();
    }

    /**
     * TPS 3000 처리를 위한 최적화된 메시지 소비
     */
    @KafkaListener(
        topics = "jmeter", 
        groupId = "${spring.kafka.consumer.group-id}",
        concurrency = "1", // 파티션 수의 2배 (3개 파티션 × 2)
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            // 메시지 카운터 증가
            consumerCounter.increment();
            
            // 메시지 엔티티 생성
            MessageEntity message = new MessageEntity();
            message.setMessageKey(record.key());
            message.setMessageValue(record.value());
            message.setTopic(record.topic());
            message.setPartitionNumber(record.partition());
            message.setOffsetNumber(record.offset());
            message.setCreatedAt(LocalDateTime.now());
            
            // 배치 처리를 위한 큐에 추가
            messageQueue.offer(message);
            
            // 오프셋 커밋을 위한 큐에 추가 (배치 처리 완료 후 커밋)
            acknowledgmentQueue.offer(acknowledgment);
            
            // 처리된 메시지 수 증가
            long count = processedCount.incrementAndGet();
            
            // 1000개마다 로그 출력
            if (count % 1000 == 0) {
                log.info("처리된 메시지 수: {}, 큐 크기: {}", count, messageQueue.size());
            }
            
        } catch (Exception e) {
            log.error("메시지 처리 중 오류 발생: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 배치 처리 스레드 (TPS 3000 처리용)
     */
    private void startBatchProcessor() {
        Thread batchProcessor = new Thread(() -> {
            List<MessageEntity> batch = new ArrayList<>();
            long lastProcessTime = System.currentTimeMillis();
            
            while (true) {
                try {
                    // 큐에서 메시지 가져오기 (최대 1000개 또는 100ms 대기)
                    MessageEntity message = messageQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                    
                    if (message != null) {
                        batch.add(message);
                    }
                    
                    long currentTime = System.currentTimeMillis();
                    boolean shouldProcess = batch.size() >= 1000 || // 1000개 모이면
                                         (!batch.isEmpty() && (currentTime - lastProcessTime) >= 100); // 100ms 경과하면
                    
                    if (shouldProcess && !batch.isEmpty()) {
                        processBatch(batch);
                        batch.clear();
                        lastProcessTime = currentTime;
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("배치 처리 중 오류 발생: {}", e.getMessage(), e);
                }
            }
        });
        
        batchProcessor.setName("batch-processor");
        batchProcessor.setDaemon(true);
        batchProcessor.start();
    }
    
    /**
     * 배치 DB 삽입 처리
     */
    @Transactional
    public void processBatch(List<MessageEntity> messages) {
        try {
            // 배치 삽입
            messageRepository.saveAll(messages);
            
            // DB 삽입 카운터 증가
            dbInsertCounter.increment(messages.size());
            
            // 배치 카운터 증가
            long batchNum = batchCount.incrementAndGet();
            
            if (batchNum % 10 == 0) { // 10배치마다 로그
                log.info("배치 처리 완료: {}개 메시지, 총 배치 수: {}", messages.size(), batchNum);
            }
            
            // 배치 처리 완료 후 오프셋 커밋
            commitOffsets(messages.size());
            
        } catch (Exception e) {
            log.error("배치 DB 삽입 중 오류 발생: {}", e.getMessage(), e);
            
            // DB 삽입 실패 시 DLQ로 전송
            sendToDLQ(messages, e);
        }
    }
    
    /**
     * 배치 처리 완료 후 오프셋 커밋
     */
    private void commitOffsets(int batchSize) {
        try {
            // 처리된 메시지 수만큼 오프셋 커밋
            for (int i = 0; i < batchSize; i++) {
                Acknowledgment ack = acknowledgmentQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (ack != null) {
                    ack.acknowledge();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("오프셋 커밋 중 인터럽트 발생: {}", e.getMessage());
        } catch (Exception e) {
            log.error("오프셋 커밋 중 오류 발생: {}", e.getMessage());
        }
    }
    
    /**
     * DLQ로 메시지 전송
     */
    private void sendToDLQ(List<MessageEntity> messages, Exception error) {
        try {
            String dlqTopic = "jmeter-dlq";
            
            for (MessageEntity message : messages) {
                // DLQ 메시지 구조 생성
                DLQMessage dlqMessage = new DLQMessage();
                dlqMessage.setOriginalTopic(message.getTopic());
                dlqMessage.setOriginalPartition(message.getPartitionNumber());
                dlqMessage.setOriginalOffset(message.getOffsetNumber());
                dlqMessage.setOriginalKey(message.getMessageKey());
                dlqMessage.setOriginalValue(message.getMessageValue());
                dlqMessage.setErrorMessage(error.getMessage());
                dlqMessage.setErrorTimestamp(LocalDateTime.now());
                dlqMessage.setRetryCount(0);
                
                // DLQ 토픽으로 전송
                kafkaTemplate.send(dlqTopic, message.getMessageKey(), dlqMessage);
                
                // DLQ 카운터 증가
                dlqCounter.increment();
            }
            
            log.warn("{}개 메시지를 DLQ로 전송했습니다. 토픽: {}", messages.size(), dlqTopic);
            
        } catch (Exception e) {
            log.error("DLQ 전송 중 오류 발생: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 현재 처리 상태 조회
     */
    public String getStatus() {
        return String.format("처리된 메시지: %d, 큐 크기: %d, 배치 수: %d", 
                processedCount.get(), messageQueue.size(), batchCount.get());
    }

    /**
     * GC 상태 로그 출력 (Controller에서 호출)
     */
    public void logMemoryStatus() {
        gcMonitoringService.logMemoryStatus();
    }
    
    /**
     * DLQ 메시지 구조
     */
    public static class DLQMessage {
        private String originalTopic;
        private Integer originalPartition;
        private Long originalOffset;
        private String originalKey;
        private String originalValue;
        private String errorMessage;
        private LocalDateTime errorTimestamp;
        private Integer retryCount;
        
        // Getters and Setters
        public String getOriginalTopic() { return originalTopic; }
        public void setOriginalTopic(String originalTopic) { this.originalTopic = originalTopic; }
        
        public Integer getOriginalPartition() { return originalPartition; }
        public void setOriginalPartition(Integer originalPartition) { this.originalPartition = originalPartition; }
        
        public Long getOriginalOffset() { return originalOffset; }
        public void setOriginalOffset(Long originalOffset) { this.originalOffset = originalOffset; }
        
        public String getOriginalKey() { return originalKey; }
        public void setOriginalKey(String originalKey) { this.originalKey = originalKey; }
        
        public String getOriginalValue() { return originalValue; }
        public void setOriginalValue(String originalValue) { this.originalValue = originalValue; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public LocalDateTime getErrorTimestamp() { return errorTimestamp; }
        public void setErrorTimestamp(LocalDateTime errorTimestamp) { this.errorTimestamp = errorTimestamp; }
        
        public Integer getRetryCount() { return retryCount; }
        public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }
    }
}

