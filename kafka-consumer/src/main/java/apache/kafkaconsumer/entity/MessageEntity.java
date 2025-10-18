package apache.kafkaconsumer.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "kafka_messages")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageEntity {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "message_key", length = 255)
    private String messageKey;
    
    @Column(name = "message_value", columnDefinition = "TEXT")
    private String messageValue;
    
    @Column(name = "topic", length = 100)
    private String topic;
    
    @Column(name = "partition_number")
    private Integer partitionNumber;
    
    @Column(name = "offset_number")
    private Long offsetNumber;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
