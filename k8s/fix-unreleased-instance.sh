#!/bin/bash

# UnreleasedInstanceIdException 해결 스크립트

BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092"

echo "=== UnreleasedInstanceIdException 해결 ==="

echo "1. Consumer Group 목록 확인:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --list

echo ""
echo "2. Consumer Group 강제 삭제:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-group --delete
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-queue-group --delete

echo ""
echo "3. 삭제 확인:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --list

echo ""
echo "✅ 완료! 이제 파드를 재시작하면 정상적으로 조인할 수 있습니다."


