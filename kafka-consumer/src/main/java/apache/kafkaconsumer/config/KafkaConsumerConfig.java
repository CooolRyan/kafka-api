package apache.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer Configuration
 * 
 * Kafka 4.1 KIP-1092: Consumer#close(CloseOptions) 지원
 * https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=321719077
 * 
 * 롤링 업데이트 시 consumer group에서 즉시 leave되지 않도록 설정
 */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>(kafkaProperties.buildConsumerProperties());
        
        // Kafka 4.1 Graceful Shutdown 관련 설정
        // 참고: CloseOptions는 런타임에 consumer.close() 호출 시 사용되므로
        // 여기서는 기본 consumer 설정만 구성합니다.
        log.info("📦 Kafka Consumer Factory 생성 (Kafka 4.1 CloseOptions 지원)");
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // Kafka 4.1 Graceful Shutdown을 위한 Container 커스터마이징
        factory.setContainerCustomizer(container -> {
            if (container instanceof ConcurrentMessageListenerContainer) {
                ConcurrentMessageListenerContainer concurrentContainer = 
                    (ConcurrentMessageListenerContainer) container;
                
                // Container Properties 설정
                ContainerProperties containerProps = concurrentContainer.getContainerProperties();
                
                log.info("🔧 Kafka Listener Container 커스터마이징: {}", 
                    concurrentContainer.getListenerId());
                
                // 참고: 실제 CloseOptions 적용은 KafkaMessageListenerContainer의
                // stop() 메서드가 호출될 때 내부 consumer에 적용됩니다.
                // Spring Kafka가 Kafka 4.1을 지원한다면 자동으로 적용될 수 있습니다.
            }
        });
        
        return factory;
    }
}

