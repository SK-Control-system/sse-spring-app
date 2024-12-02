# 1. Base image 설정
FROM openjdk:17-jdk-slim

RUN apt-get update && apt-get install -y curl nano && rm -rf /var/lib/apt/lists/*
# 2. 애플리케이션 실행 디렉토리 설정
WORKDIR /app

# 3. Build 결과물 복사
COPY build/libs/*.jar app.jar

# 4. 컨테이너 실행 시 실행할 명령 설정
ENTRYPOINT ["java", "-jar", "app.jar"]
