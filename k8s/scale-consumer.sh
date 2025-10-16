#!/bin/bash

# Kafka Consumer 스케일링 스크립트

REPLICAS=$1

if [ -z "$REPLICAS" ]; then
    echo "❌ 사용법: $0 [replicas]"
    echo "  예: $0 1    # 1개 파드"
    echo "  예: $0 3    # 3개 파드"
    echo "  예: $0 0    # 모든 파드 중지"
    exit 1
fi

echo "🔄 Consumer 파드 수를 $REPLICAS 개로 조절 중..."

# Consumer 스케일링
kubectl scale deployment kafka-consumer --replicas=$REPLICAS -n kafka-microservices

# 상태 확인
echo "📊 현재 Consumer 파드 상태:"
kubectl get pods -n kafka-microservices | grep kafka-consumer

# Consumer Group 상태 확인 (파드가 실행 중일 때만)
if [ "$REPLICAS" -gt 0 ]; then
    echo "⏳ 파드 시작 대기 중... (10초)"
    sleep 10
    
    echo "📊 Consumer Group 상태:"
    kubectl exec -it kafka-consumer-xxx -n kafka-microservices -- kafka-consumer-groups.sh \
      --bootstrap-server kafka1:9092 \
      --group kafka-consumer-group \
      --describe 2>/dev/null || echo "Consumer Group 정보를 가져올 수 없습니다."
fi

echo "✅ 스케일링 완료!"

