# 1단계: Gradle을 사용하여 애플리케이션 빌드
FROM gradle:8.7-jdk17-alpine AS builder
WORKDIR /app
COPY build.gradle settings.gradle /app/
COPY src /app/src
RUN gradle build -x test --no-daemon

# 2단계: 실제 실행을 위한 최소한의 환경 구성
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
