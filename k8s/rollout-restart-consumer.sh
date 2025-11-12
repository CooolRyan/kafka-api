#!/bin/bash

# Kafka Consumer StatefulSet 롤링 재시작 스크립트
# StatefulSet은 kubectl rollout restart를 지원하지 않으므로
# annotation을 추가하여 강제 재시작

NAMESPACE="kafka-microservices"
STATEFULSET_NAME="kafka-consumer"

echo "🔄 Kafka Consumer StatefulSet 롤링 재시작 시작..."

# 현재 시간을 annotation에 추가하여 강제 재시작 트리거
kubectl patch statefulset ${STATEFULSET_NAME} -n ${NAMESPACE} \
  -p '{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"'$(date +%Y-%m-%dT%H:%M:%S%z)'"}}}}}'

echo "✅ 롤링 재시작 트리거 완료"
echo ""
echo "📊 상태 확인:"
kubectl get pods -n ${NAMESPACE} -l app=${STATEFULSET_NAME} -w

