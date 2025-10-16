#!/bin/bash

# Kafka Consumer Group 리밸런싱 테스트 스크립트

echo "🚀 Kafka Consumer Group 리밸런싱 테스트 시작!"

# 1. 초기 상태 (1개 파드)
echo "📊 1단계: 1개 파드로 시작"
kubectl scale deployment kafka-consumer --replicas=1 -n kafka-microservices
sleep 15

echo "📊 Consumer Group 상태 (1개 파드):"
kubectl exec -it kafka-consumer-xxx -n kafka-microservices -- kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --group kafka-consumer-group \
  --describe 2>/dev/null || echo "Consumer Group 정보를 가져올 수 없습니다."

# 2. 3개 파드로 스케일 아웃
echo "📊 2단계: 3개 파드로 스케일 아웃"
kubectl scale deployment kafka-consumer --replicas=3 -n kafka-microservices
sleep 15

echo "📊 Consumer Group 상태 (3개 파드):"
kubectl exec -it kafka-consumer-xxx -n kafka-microservices -- kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --group kafka-consumer-group \
  --describe 2>/dev/null || echo "Consumer Group 정보를 가져올 수 없습니다."

# 3. 2개 파드로 스케일 인
echo "📊 3단계: 2개 파드로 스케일 인"
kubectl scale deployment kafka-consumer --replicas=2 -n kafka-microservices
sleep 15

echo "📊 Consumer Group 상태 (2개 파드):"
kubectl exec -it kafka-consumer-xxx -n kafka-microservices -- kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --group kafka-consumer-group \
  --describe 2>/dev/null || echo "Consumer Group 정보를 가져올 수 없습니다."

# 4. 1개 파드로 다시 스케일 인
echo "📊 4단계: 1개 파드로 다시 스케일 인"
kubectl scale deployment kafka-consumer --replicas=1 -n kafka-microservices
sleep 15

echo "📊 Consumer Group 상태 (1개 파드):"
kubectl exec -it kafka-consumer-xxx -n kafka-microservices -- kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --group kafka-consumer-group \
  --describe 2>/dev/null || echo "Consumer Group 정보를 가져올 수 없습니다."

echo "✅ 리밸런싱 테스트 완료!"

