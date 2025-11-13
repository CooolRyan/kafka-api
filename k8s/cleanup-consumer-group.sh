#!/bin/bash

# Consumer Group 정리 스크립트
# 이전 파드가 완전히 종료되지 않아 consumer group에 남아있는 경우 사용

NAMESPACE="kafka-microservices"
STATEFULSET_NAME="kafka-consumer"

echo "=== Consumer Group 정리 ==="
echo "1. StatefulSet 스케일 다운 (모든 파드 삭제)"
kubectl scale statefulset "$STATEFULSET_NAME" -n "$NAMESPACE" --replicas=0

echo "2. 파드 완전 삭제 대기 (30초)"
sleep 30

echo "3. Consumer Group 상태 확인"
echo "   Kafka 서버에서 다음 명령어 실행:"
echo "   ./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group kafka-consumer-group --describe"
echo "   ./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group kafka-consumer-queue-group --describe"

echo "4. 필요시 Consumer Group 삭제 (주의: 오프셋 정보도 삭제됨)"
echo "   ./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group kafka-consumer-group --delete"
echo "   ./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group kafka-consumer-queue-group --delete"

echo "5. StatefulSet 재시작"
kubectl scale statefulset "$STATEFULSET_NAME" -n "$NAMESPACE" --replicas=2

echo "=== 완료 ==="


