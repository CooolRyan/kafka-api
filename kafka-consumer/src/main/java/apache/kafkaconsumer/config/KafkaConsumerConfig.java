package apache.kafkaconsumer.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Kafka Consumer 설정
 */
@Configuration
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
            kafkaProperties.buildConsumerProperties()
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // 배치 리스너 활성화 (application.yml의 type: batch와 함께 사용)
        factory.setBatchListener(true);
        
        // 수동 커밋 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        log.info("✅ KafkaListenerContainerFactory 설정 완료");
        log.info("   - Batch Listener: 활성화");
        log.info("   - Ack Mode: MANUAL");
        
        return factory;
    }
}

