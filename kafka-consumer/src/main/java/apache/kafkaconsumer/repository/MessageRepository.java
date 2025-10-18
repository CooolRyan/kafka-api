package apache.kafkaconsumer.repository;

import apache.kafkaconsumer.entity.MessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MessageRepository extends JpaRepository<MessageEntity, Long> {
    
    /**
     * 배치 삽입을 위한 네이티브 쿼리
     */
    @Modifying
    @Query(value = "INSERT INTO kafka_messages (message_key, message_value, topic, partition_number, offset_number, created_at) " +
                   "VALUES (:#{#message.messageKey}, :#{#message.messageValue}, :#{#message.topic}, " +
                   ":#{#message.partitionNumber}, :#{#message.offsetNumber}, :#{#message.createdAt})", 
           nativeQuery = true)
    void insertMessage(@Param("message") MessageEntity message);
    
    /**
     * 토픽별 메시지 수 조회
     */
    @Query("SELECT COUNT(m) FROM MessageEntity m WHERE m.topic = :topic")
    long countByTopic(@Param("topic") String topic);
    
    /**
     * 최근 N개 메시지 조회
     */
    @Query("SELECT m FROM MessageEntity m ORDER BY m.createdAt DESC")
    List<MessageEntity> findRecentMessages();
}
