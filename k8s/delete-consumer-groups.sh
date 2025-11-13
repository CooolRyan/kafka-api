#!/bin/bash

# Consumer Group 삭제 스크립트
# 이전 파드가 남긴 consumer group 정보를 완전히 삭제

BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092"

echo "=== Consumer Group 삭제 ==="

# Consumer Group 목록 확인
echo "1. Consumer Group 목록 확인:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --list

echo ""
echo "2. Consumer Group 상태 확인:"
echo "   kafka-consumer-group:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-group --describe

echo ""
echo "   kafka-consumer-queue-group:"
./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-queue-group --describe

echo ""
echo "3. Consumer Group 삭제 (주의: 오프셋 정보도 삭제됨):"
read -p "   kafka-consumer-group을 삭제하시겠습니까? (y/N): " confirm1
if [ "$confirm1" = "y" ] || [ "$confirm1" = "Y" ]; then
    ./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-group --delete
    echo "   ✅ kafka-consumer-group 삭제 완료"
fi

read -p "   kafka-consumer-queue-group을 삭제하시겠습니까? (y/N): " confirm2
if [ "$confirm2" = "y" ] || [ "$confirm2" = "Y" ]; then
    ./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --group kafka-consumer-queue-group --delete
    echo "   ✅ kafka-consumer-queue-group 삭제 완료"
fi

echo ""
echo "=== 완료 ==="

