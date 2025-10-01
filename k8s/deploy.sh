#!/bin/bash

# Kubernetes 배포 스크립트
echo "🚀 Kafka Microservices 배포 시작..."

# 네임스페이스 생성
echo "📦 네임스페이스 생성 중..."
kubectl apply -f namespace.yaml
kubectl apply -f kafka-gateway-namespace.yaml

# ConfigMap 생성
echo "⚙️ ConfigMap 생성 중..."
kubectl apply -f configmap.yaml

# 서비스들 배포
echo "🔧 서비스들 배포 중..."
kubectl apply -f kafka-producer-deployment.yaml
kubectl apply -f kafka-consumer-deployment.yaml
kubectl apply -f kafka-api-deployment.yaml

# 배포 상태 확인
echo "📊 배포 상태 확인 중..."
echo "🔧 Microservices (Producer/Consumer):"
kubectl get pods -n kafka-microservices
kubectl get services -n kafka-microservices
echo ""
echo "🌐 Gateway (API):"
kubectl get pods -n kafka-gateway
kubectl get services -n kafka-gateway

echo "✅ 배포 완료!"
echo ""
echo "🔍 상태 확인 명령어:"
echo "kubectl get pods -n kafka-microservices"
echo "kubectl get pods -n kafka-gateway"
echo "kubectl logs -f deployment/kafka-api -n kafka-gateway"
echo ""
echo "🌐 API 접근 (Ingress 설정 후):"
echo "http://kafka-api.local"
