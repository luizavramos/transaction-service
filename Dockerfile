FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/transaction-service.jar app.jar
EXPOSE 8086
ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=60", "-jar", "/app/app.jar"]
