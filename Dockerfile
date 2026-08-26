FROM gcr.io/distroless/java25-debian13

ARG JAR_FILE=build/libs/*.jar
COPY ${JAR_FILE} /app/app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-XX:-OmitStackTraceInFastThrow", "-jar", "/app/app.jar"]
