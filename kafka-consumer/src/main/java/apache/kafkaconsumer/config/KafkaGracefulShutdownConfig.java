package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka Graceful Shutdown Configuration
 * 
 * Spring Kafka의 기본 종료 프로세스를 사용하여 graceful shutdown 수행
 * 
 * Static Group Membership (group.instance.id)과 함께 사용하면
 * 롤링 업데이트 시 파티션 할당이 유지됨
 * 
 * SmartLifecycle을 사용하여 Spring Kafka의 기본 종료 프로세스보다 먼저 실행되도록 함
 * (phase를 낮게 설정하여 다른 Lifecycle보다 먼저 stop됨)
 * 
 * 참고: Kafka 4.1 CloseOptions는 Spring Kafka의 기본 종료 프로세스와 충돌할 수 있어
 * 현재는 사용하지 않음. 대신 Spring Kafka의 기본 종료 프로세스를 사용하며,
 * terminationGracePeriodSeconds 동안 대기합니다.
 */
@Component
@Slf4j
public class KafkaGracefulShutdownConfig implements SmartLifecycle {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public KafkaGracefulShutdownConfig(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        if (!running.getAndSet(false)) {
            return; // 이미 종료됨
        }

        log.info("🛑 Kafka Consumer graceful shutdown 시작...");
        log.info("   Spring Kafka의 기본 종료 프로세스를 사용합니다.");
        log.info("   Static Group Membership (group.instance.id)으로 파티션 할당이 유지됩니다.");
        
        // 모든 Kafka Listener Container 중지
        Collection<MessageListenerContainer> containers = kafkaListenerEndpointRegistry.getAllListenerContainers();
        
        for (MessageListenerContainer container : containers) {
            if (container.isRunning()) {
                log.info("📦 Listener Container '{}' 종료 중...", container.getListenerId());
                
                try {
                    // Spring Kafka의 기본 종료 프로세스 사용
                    // container.stop()을 호출하면 Spring Kafka가 내부적으로
                    // 모든 하위 container와 consumer를 안전하게 종료함
                    container.stop();
                    
                    log.info("✅ Listener Container '{}' 종료 완료", container.getListenerId());
                } catch (Exception e) {
                    log.error("❌ Listener Container '{}' 종료 중 오류 발생: {}", 
                        container.getListenerId(), e.getMessage(), e);
                    // 이미 종료 중일 수 있으므로 무시
                }
            }
        }
        
        log.info("🎯 모든 Kafka Consumer graceful shutdown 완료");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * phase를 낮게 설정하여 다른 Lifecycle Bean들보다 먼저 stop되도록 함
     * KafkaListenerEndpointRegistry의 기본 phase는 Integer.MAX_VALUE이므로
     * 이 값보다 낮게 설정하면 먼저 실행됨
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }

    /**
     * 참고: CloseOptions는 Spring Kafka의 기본 종료 프로세스와 충돌할 수 있으므로
     * 현재는 사용하지 않음
     * 
     * 대신 Spring Kafka의 기본 종료 프로세스를 사용하며,
     * Static Group Membership (group.instance.id)과 함께 사용하면
     * 파티션 할당이 유지됩니다.
     * 
     * Spring Kafka는 자동으로 graceful shutdown을 처리하며,
     * terminationGracePeriodSeconds 동안 대기합니다.
     */

}

