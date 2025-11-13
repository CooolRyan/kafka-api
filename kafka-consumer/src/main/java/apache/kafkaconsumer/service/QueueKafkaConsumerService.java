package apache.kafkaconsumer.service;

import apache.kafkaconsumer.entity.MessageEntity;
import apache.kafkaconsumer.repository.MessageRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class QueueKafkaConsumerService {

    private final MessageRepository messageRepository;
    private final MeterRegistry meterRegistry;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    // Queue 기반 메시지 처리 (기존과 분리)
    private final BlockingQueue<MessageEntity> queueMessageQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Acknowledgment> queueAcknowledgmentQueue = new LinkedBlockingQueue<>();
    private final AtomicLong queueProcessedCount = new AtomicLong(0);
    private final AtomicLong queueBatchCount = new AtomicLong(0);
    
    // 메트릭 카운터 (기존과 분리)
    private final Counter queueConsumerCounter;
    private final Counter queueDbInsertCounter;
    private final Counter queueDlqCounter;
    
    public QueueKafkaConsumerService(MessageRepository messageRepository, MeterRegistry meterRegistry, KafkaTemplate<String, Object> kafkaTemplate) {
        this.messageRepository = messageRepository;
        this.meterRegistry = meterRegistry;
        this.kafkaTemplate = kafkaTemplate;
        
        this.queueConsumerCounter = Counter.builder("queue_consumer_msg_total")
                .description("Queue 기반 컨슈머된 메시지 수")
                .register(meterRegistry);
        this.queueDbInsertCounter = Counter.builder("queue_db_insert_total")
                .description("Queue 기반 DB 삽입된 메시지 수")
                .register(meterRegistry);
        this.queueDlqCounter = Counter.builder("queue_dlq_msg_total")
                .description("Queue 기반 DLQ로 전송된 메시지 수")
                .register(meterRegistry);
        
        // Queue 기반 배치 처리 스레드 시작
        startQueueBatchProcessor();
    }

    /**
     * Queue 기반 메시지 소비 (새로운 토픽: jmeter-queue)
     */
    @KafkaListener(
        topics = "jmeter-queue",  // 새로운 토픽
        groupId = "${spring.kafka.consumer.group-id}",  // 동일한 consumer group 사용
        concurrency = "1"
    )
    public void consumeQueue(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            queueConsumerCounter.increment();

            MessageEntity message = new MessageEntity();
            message.setMessageKey(record.key());
            message.setMessageValue(record.value());
            message.setTopic(record.topic());
            message.setPartitionNumber(record.partition());
            message.setOffsetNumber(record.offset());
            message.setCreatedAt(LocalDateTime.now());
            
            // Queue에 메시지 추가 (기존과 분리된 큐 사용)
            queueMessageQueue.offer(message);
            queueAcknowledgmentQueue.offer(acknowledgment);
            
            long count = queueProcessedCount.incrementAndGet();
            if (count % 1000 == 0) {
                log.info("Queue 처리된 메시지 수: {}, 큐 크기: {}", count, queueMessageQueue.size());
            }
        } catch (Exception e) {
            log.error("Queue 메시지 처리 중 오류 발생: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Queue 기반 배치 처리 스레드 (기존과 분리)
     */
    private void startQueueBatchProcessor() {
        Thread queueBatchProcessor = new Thread(() -> {
            List<MessageEntity> batch = new ArrayList<>();
            long lastProcessTime = System.currentTimeMillis();
            
            while (true) {
                try {
                    // Queue에서 메시지 가져오기 (500ms 대기)
                    MessageEntity message = queueMessageQueue.poll(500, TimeUnit.MILLISECONDS);
                    
                    if (message != null) {
                        batch.add(message);
                    }
                    
                    long currentTime = System.currentTimeMillis();
                    boolean shouldProcess = batch.size() >= 1000 || // 1000개 모이면
                                         (!batch.isEmpty() && (currentTime - lastProcessTime) >= 1000); // 1초 경과하면
                    
                    if (shouldProcess && !batch.isEmpty()) {
                        processQueueBatch(batch);
                        batch.clear();
                        lastProcessTime = currentTime;
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Queue 배치 처리 중 오류 발생: {}", e.getMessage(), e);
                }
            }
        });
        
        queueBatchProcessor.setName("queue-batch-processor");
        queueBatchProcessor.setDaemon(true);
        queueBatchProcessor.setPriority(Thread.MAX_PRIORITY);
        queueBatchProcessor.start();
        
        log.info("Queue 기반 배치 처리 스레드가 시작되었습니다.");
    }

    /**
     * Queue 기반 배치 DB 삽입 (기존과 분리)
     */
    @Transactional
    public void processQueueBatch(List<MessageEntity> messages) {
        try {
            messageRepository.saveAll(messages);
            queueDbInsertCounter.increment(messages.size());
            long batchNum = queueBatchCount.incrementAndGet();
            
            if (batchNum % 10 == 0) {
                log.info("Queue 배치 처리 완료: {}개 메시지, 총 배치 수: {}", messages.size(), batchNum);
            }
            
            commitQueueOffsets(messages.size());
        } catch (Exception e) {
            log.error("Queue 배치 DB 삽입 중 오류 발생: {}", e.getMessage(), e);
            sendToQueueDLQ(messages, e);
        }
    }

    /**
     * Queue 기반 오프셋 커밋 (기존과 분리)
     */
    private void commitQueueOffsets(int batchSize) {
        try {
            for (int i = 0; i < batchSize; i++) {
                Acknowledgment ack = queueAcknowledgmentQueue.poll(100, TimeUnit.MILLISECONDS);
                if (ack != null) {
                    ack.acknowledge();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Queue 오프셋 커밋 중 인터럽트 발생: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Queue 오프셋 커밋 중 오류 발생: {}", e.getMessage());
        }
    }

    /**
     * Queue 기반 DLQ 전송 (기존과 분리)
     */
    private void sendToQueueDLQ(List<MessageEntity> messages, Exception error) {
        try {
            String dlqTopic = "jmeter-queue-dlq";
            for (MessageEntity message : messages) {
                DLQMessage dlqMessage = new DLQMessage();
                dlqMessage.setOriginalTopic(message.getTopic());
                dlqMessage.setOriginalPartition(message.getPartitionNumber());
                dlqMessage.setOriginalOffset(message.getOffsetNumber());
                dlqMessage.setOriginalKey(message.getMessageKey());
                dlqMessage.setOriginalValue(message.getMessageValue());
                dlqMessage.setErrorMessage(error.getMessage());
                dlqMessage.setErrorTimestamp(LocalDateTime.now());
                dlqMessage.setRetryCount(0);
                kafkaTemplate.send(dlqTopic, message.getMessageKey(), dlqMessage);
                queueDlqCounter.increment();
            }
            log.warn("{}개 메시지를 Queue DLQ로 전송했습니다. 토픽: {}", messages.size(), dlqTopic);
        } catch (Exception e) {
            log.error("Queue DLQ 전송 중 오류 발생: {}", e.getMessage(), e);
        }
    }

    /**
     * Queue 상태 조회 (기존과 분리)
     */
    public String getQueueStatus() {
        return String.format("Queue 처리된 메시지: %d, 큐 크기: %d, 배치 수: %d", 
                queueProcessedCount.get(), queueMessageQueue.size(), queueBatchCount.get());
    }
    
    /**
     * DLQ 메시지 구조 (기존과 분리)
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