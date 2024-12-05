# 1. Base image 설정 (Alpine 기반 명시)
FROM openjdk:17-jdk-alpine

# 2. curl, nano 설치 (apk 사용)
RUN apk update && apk add --no-cache curl nano

# 3. 애플리케이션 실행 디렉토리 설정
WORKDIR /app

# 4. Build 결과물 복사
COPY build/libs/demo-0.0.1-SNAPSHOT.jar app.jar

# 5. 컨테이너 실행 시 실행할 명령 설정
ENTRYPOINT ["java", "-jar", "app.jar"]
