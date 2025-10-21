#!/bin/bash

# PostgreSQL 연결 테스트 및 테이블 생성 스크립트

echo "PostgreSQL 연결 테스트 및 테이블 생성 중..."

# PostgreSQL 연결 정보
POSTGRES_HOST="192.168.211.138"
POSTGRES_PORT="5432"
POSTGRES_DATABASE="kafka_db"
POSTGRES_USERNAME="postgres"
POSTGRES_PASSWORD="postgres"

# 다른 가능한 비밀번호들 시도
POSSIBLE_PASSWORDS=("postgres" "password" "123456" "admin" "root" "")

echo "PostgreSQL 연결 정보:"
echo "Host: $POSTGRES_HOST"
echo "Port: $POSTGRES_PORT"
echo "Database: $POSTGRES_DATABASE"
echo "Username: $POSTGRES_USERNAME"
echo ""

# 먼저 데이터베이스 존재 여부 확인
echo "데이터베이스 존재 여부 확인..."
for password in "${POSSIBLE_PASSWORDS[@]}"; do
    echo "비밀번호 '$password' 시도 중..."
    PGPASSWORD="$password" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d postgres -c "SELECT 1;" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "연결 성공! 비밀번호: '$password'"
        POSTGRES_PASSWORD="$password"
        break
    fi
done

if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "모든 비밀번호 시도 실패. 수동으로 확인하세요."
    echo "다음 명령어로 직접 연결해보세요:"
    echo "psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d postgres"
    exit 1
fi

# 데이터베이스 생성 (존재하지 않는 경우)
echo "데이터베이스 생성 확인..."
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d postgres -c "CREATE DATABASE $POSTGRES_DATABASE;" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "데이터베이스 '$POSTGRES_DATABASE' 생성됨"
else
    echo "데이터베이스 '$POSTGRES_DATABASE' 이미 존재하거나 생성 실패"
fi

# 연결 테스트
echo "PostgreSQL 연결 테스트..."
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d $POSTGRES_DATABASE -c "SELECT version();"

if [ $? -eq 0 ]; then
    echo "PostgreSQL 연결 성공!"
    
    # 테이블 생성
    echo "테이블 생성 중..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d $POSTGRES_DATABASE -f k8s/postgres-init.sql
    
    if [ $? -eq 0 ]; then
        echo "테이블 생성 완료!"
        
        # 테이블 확인
        echo "생성된 테이블 확인..."
        PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d $POSTGRES_DATABASE -c "\dt"
        
        # 테이블 구조 확인
        echo "테이블 구조 확인..."
        PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d $POSTGRES_DATABASE -c "\d kafka_messages"
    else
        echo "테이블 생성 실패!"
        exit 1
    fi
else
    echo "PostgreSQL 연결 실패!"
    echo "다음을 확인하세요:"
    echo "1. PostgreSQL 서버가 실행 중인지"
    echo "2. 네트워크 연결이 가능한지"
    echo "3. 방화벽 설정이 올바른지"
    echo "4. 데이터베이스와 사용자가 존재하는지"
    exit 1
fi

echo "PostgreSQL 설정 완료!"
