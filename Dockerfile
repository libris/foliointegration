FROM sourcemation/jdk-25
COPY ./build/libs/foliolibrisintegration.jar .
EXPOSE 8080
CMD ["java", "-jar", "foliolibrisintegration.jar"]
