# Kafka Microservices 배포 가이드

## 🚀 GitHub Actions + Docker Hub + Kubernetes 배포

### 1. Docker Hub 설정

#### 1.1 Docker Hub 계정 생성 및 리포지토리 생성
1. [Docker Hub](https://hub.docker.com)에서 계정 생성
2. 다음 리포지토리들을 생성:
   - `YOUR_USERNAME/kafka-producer`
   - `YOUR_USERNAME/kafka-consumer` 
   - `YOUR_USERNAME/kafka-api`

#### 1.2 GitHub Secrets 설정
GitHub 리포지토리 → Settings → Secrets and variables → Actions에서 다음 시크릿 추가:

```
DOCKER_USERNAME: your_dockerhub_username
DOCKER_PASSWORD: your_dockerhub_token_or_password
```

### 2. GitHub Actions 워크플로우

각 서비스별로 자동 빌드 및 푸시가 설정되어 있습니다:

- **kafka-producer**: `kafka-producer/` 디렉토리 변경 시 트리거
- **kafka-consumer**: `kafka-consumer/` 디렉토리 변경 시 트리거  
- **kafka-api**: 루트 디렉토리 변경 시 트리거

### 3. Kubernetes 배포

#### 3.1 이미지 태그 수정
`k8s/` 디렉토리의 모든 YAML 파일에서 `YOUR_DOCKERHUB_USERNAME`을 실제 Docker Hub 사용자명으로 변경:

```yaml
image: YOUR_DOCKERHUB_USERNAME/kafka-producer:latest
```

#### 3.2 배포 실행
```bash
# 네임스페이스 생성
kubectl apply -f k8s/namespace.yaml          # kafka-microservices
kubectl apply -f k8s/kafka-gateway-namespace.yaml  # kafka-gateway

# ConfigMap 생성
kubectl apply -f k8s/configmap.yaml

# 서비스들 배포
kubectl apply -f k8s/kafka-producer-deployment.yaml  # kafka-microservices
kubectl apply -f k8s/kafka-consumer-deployment.yaml  # kafka-microservices
kubectl apply -f k8s/kafka-api-deployment.yaml       # kafka-gateway

# 또는 배포 스크립트 실행 (Linux/Mac)
./k8s/deploy.sh
```

#### 3.3 배포 상태 확인
```bash
# Microservices (Producer/Consumer) 상태 확인
kubectl get pods -n kafka-microservices
kubectl get services -n kafka-microservices

# Gateway (API) 상태 확인
kubectl get pods -n kafka-gateway
kubectl get services -n kafka-gateway

# 로그 확인
kubectl logs -f deployment/kafka-api -n kafka-gateway
```

### 4. 개발 워크플로우

1. **코드 변경** → GitHub에 푸시
2. **GitHub Actions** → 자동으로 Docker 이미지 빌드 및 푸시
3. **로컬 K8s** → 이미지 태그 업데이트 후 재배포

### 5. 이미지 태그 업데이트

새로운 이미지가 푸시되면 K8s에서 이미지를 업데이트하려면:

```bash
# Microservices 재시작
kubectl rollout restart deployment/kafka-producer -n kafka-microservices
kubectl rollout restart deployment/kafka-consumer -n kafka-microservices

# Gateway 재시작
kubectl rollout restart deployment/kafka-api -n kafka-gateway

# 또는 이미지 태그 직접 업데이트
kubectl set image deployment/kafka-producer kafka-producer=YOUR_USERNAME/kafka-producer:latest -n kafka-microservices
kubectl set image deployment/kafka-api kafka-api=YOUR_USERNAME/kafka-api:latest -n kafka-gateway
```

### 6. 환경별 설정

#### 6.1 개발 환경
- 브랜치: `develop`
- 이미지 태그: `develop` 또는 `latest`

#### 6.2 운영 환경  
- 브랜치: `master`
- 이미지 태그: `master` 또는 `latest`

### 7. 모니터링

```bash
# Microservices 리소스 사용량 확인
kubectl top pods -n kafka-microservices

# Gateway 리소스 사용량 확인
kubectl top pods -n kafka-gateway

# 이벤트 확인
kubectl get events -n kafka-microservices --sort-by='.lastTimestamp'
kubectl get events -n kafka-gateway --sort-by='.lastTimestamp'

# 서비스 엔드포인트 확인
kubectl get endpoints -n kafka-microservices
kubectl get endpoints -n kafka-gateway
```

### 8. 트러블슈팅

#### 8.1 이미지 Pull 실패
```bash
# Microservices 이미지 확인
kubectl describe pod <pod-name> -n kafka-microservices

# Gateway 이미지 확인
kubectl describe pod <pod-name> -n kafka-gateway

# 시크릿 확인
kubectl get secrets -n kafka-microservices
kubectl get secrets -n kafka-gateway
```

#### 8.2 서비스 연결 문제
```bash
# Microservices 서비스 DNS 확인
kubectl run debug --image=busybox -it --rm -- nslookup kafka-producer-service.kafka-microservices.svc.cluster.local

# Gateway 서비스 DNS 확인
kubectl run debug --image=busybox -it --rm -- nslookup kafka-api-service.kafka-gateway.svc.cluster.local
```

### 9. 확장성

#### 9.1 수평 확장
```bash
# Microservices Replica 수 증가
kubectl scale deployment kafka-producer --replicas=3 -n kafka-microservices
kubectl scale deployment kafka-consumer --replicas=3 -n kafka-microservices

# Gateway Replica 수 증가
kubectl scale deployment kafka-api --replicas=3 -n kafka-gateway
```

#### 9.2 리소스 제한 조정
`k8s/*-deployment.yaml` 파일에서 `resources` 섹션 수정

---

## 📝 참고사항

- **Microservices** (`kafka-producer`, `kafka-consumer`)는 `kafka-microservices` 네임스페이스에 배포됩니다
- **Gateway** (`kafka-api`)는 `kafka-gateway` 네임스페이스에 배포됩니다
- Health check는 `/actuator/health` 엔드포인트를 사용합니다
- Ingress는 `kafka-api.local` 도메인으로 설정되어 있습니다
- 각 서비스는 독립적으로 스케일링 가능합니다
- 네임스페이스 분리로 인해 API Gateway와 Microservices 간의 명확한 역할 분리가 가능합니다
