#!/bin/bash

# Consumer Group 강제 삭제 스크립트
# UnreleasedInstanceIdException 해결용

BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092"

echo "=== Consumer Group 강제 삭제 ==="

echo "1. Consumer Group 목록 확인:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --list

echo ""
echo "2. Consumer Group 강제 삭제:"
echo "   kafka-consumer-group 삭제 중..."
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-group --delete
echo "   ✅ kafka-consumer-group 삭제 완료"

echo ""
echo "   kafka-consumer-queue-group 삭제 중..."
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-queue-group --delete
echo "   ✅ kafka-consumer-queue-group 삭제 완료"

echo ""
echo "3. 삭제 확인:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --list

echo ""
echo "=== 완료 ==="
echo "이제 파드를 재시작하면 정상적으로 조인할 수 있습니다."


