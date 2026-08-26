FROM sourcemation/jdk-25
COPY ./build/libs/foliointegration.jar .
EXPOSE 8080
CMD ["java", "-XX:-OmitStackTraceInFastThrow", "-jar", "foliointegration.jar"]