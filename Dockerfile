FROM alpine/java:22-jdk
COPY ./build/libs/foliolibrisintegration.jar .
EXPOSE 8080
CMD ["java", "-jar", "foliolibrisintegration.jar"]
