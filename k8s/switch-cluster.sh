#!/bin/bash

# Kafka 클러스터 스위칭 스크립트

CLUSTER=$1

if [ "$CLUSTER" = "1" ]; then
    echo "🔄 Switching to Cluster 1 (기존 3.8 버전)..."
    
    # Cluster 1 배포
    kubectl apply -f k8s/configmap-cluster1.yaml --validate=false
    kubectl apply -f k8s/kafka-consumer-deployment-cluster1.yaml --validate=false
    kubectl apply -f k8s/kafka-producer-deployment-cluster1.yaml --validate=false
    
    # Cluster 2 중지
    kubectl delete deployment kafka-consumer-cluster2 -n kafka-microservices --ignore-not-found=true
    kubectl delete deployment kafka-producer-cluster2 -n kafka-microservices --ignore-not-found=true
    
    echo "✅ Cluster 1 활성화 완료!"
    
elif [ "$CLUSTER" = "2" ]; then
    echo "🔄 Switching to Cluster 2 (새로운 3.9 버전)..."
    
    # Cluster 2 배포
    kubectl apply -f k8s/configmap-cluster2.yaml --validate=false
    kubectl apply -f k8s/kafka-consumer-deployment-cluster2.yaml --validate=false
    kubectl apply -f k8s/kafka-producer-deployment-cluster2.yaml --validate=false
    
    # Cluster 1 중지
    kubectl delete deployment kafka-consumer-cluster1 -n kafka-microservices --ignore-not-found=true
    kubectl delete deployment kafka-producer-cluster1 -n kafka-microservices --ignore-not-found=true
    
    echo "✅ Cluster 2 활성화 완료!"
    
else
    echo "❌ 사용법: $0 [1|2]"
    echo "  1: Cluster 1 (기존 3.8 버전)"
    echo "  2: Cluster 2 (새로운 3.9 버전)"
    exit 1
fi

# 상태 확인
echo "📊 현재 Pod 상태:"
kubectl get pods -n kafka-microservices | grep kafka-consumer
kubectl get pods -n kafka-microservices | grep kafka-producer
