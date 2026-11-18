# Build the jars on the host first: ./mvnw -B -q package -DskipTests
# Usage: docker build -f docker/service.Dockerfile --build-arg MODULE=order-service .
FROM eclipse-temurin:17.0.21_9-jre
ARG MODULE
COPY ${MODULE}/target/${MODULE}.jar /app/app.jar
ENTRYPOINT ["java","-XX:MaxRAMPercentage=75","-jar","/app/app.jar"]
