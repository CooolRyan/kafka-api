-- PostgreSQL 테이블 생성 스크립트
-- kafka_db 데이터베이스에서 실행

-- 데이터베이스 생성 (필요한 경우)
-- CREATE DATABASE kafka_db;

-- 테이블 생성
CREATE TABLE IF NOT EXISTS kafka_messages (
    id BIGSERIAL PRIMARY KEY,
    message_key VARCHAR(255),
    message_value TEXT,
    topic VARCHAR(100),
    partition_number INTEGER,
    offset_number BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성 (성능 최적화)
CREATE INDEX IF NOT EXISTS idx_kafka_messages_topic ON kafka_messages(topic);
CREATE INDEX IF NOT EXISTS idx_kafka_messages_created_at ON kafka_messages(created_at);
CREATE INDEX IF NOT EXISTS idx_kafka_messages_topic_partition_offset ON kafka_messages(topic, partition_number, offset_number);

-- 테이블 정보 확인
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'kafka_messages'
ORDER BY ordinal_position;
