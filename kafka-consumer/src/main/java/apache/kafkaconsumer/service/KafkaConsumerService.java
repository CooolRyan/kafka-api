package apache.kafkaconsumer.service;

import apache.kafkaconsumer.entity.MessageEntity;
import apache.kafkaconsumer.repository.MessageRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
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
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final MessageRepository messageRepository;
    private final MeterRegistry meterRegistry;
    
    // 배치 처리를 위한 큐
    private final BlockingQueue<MessageEntity> messageQueue = new LinkedBlockingQueue<>();
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong batchCount = new AtomicLong(0);
    
    // 메트릭 카운터
    private final Counter consumerCounter;
    private final Counter dbInsertCounter;
    
    public KafkaConsumerService(MessageRepository messageRepository, MeterRegistry meterRegistry) {
        this.messageRepository = messageRepository;
        this.meterRegistry = meterRegistry;
        this.consumerCounter = Counter.builder("consumer_msg_total")
                .description("전체 컨슈머된 메시지 수")
                .register(meterRegistry);
        this.dbInsertCounter = Counter.builder("db_insert_total")
                .description("전체 DB 삽입된 메시지 수")
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
        concurrency = "6", // 파티션 수의 2배 (3개 파티션 × 2)
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
            
            // 처리된 메시지 수 증가
            long count = processedCount.incrementAndGet();
            
            // 1000개마다 로그 출력
            if (count % 1000 == 0) {
                log.info("처리된 메시지 수: {}, 큐 크기: {}", count, messageQueue.size());
            }
            
            // 수동 커밋
            acknowledgment.acknowledge();
            
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
            
        } catch (Exception e) {
            log.error("배치 DB 삽입 중 오류 발생: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 현재 처리 상태 조회
     */
    public String getStatus() {
        return String.format("처리된 메시지: %d, 큐 크기: %d, 배치 수: %d", 
                processedCount.get(), messageQueue.size(), batchCount.get());
    }
}

