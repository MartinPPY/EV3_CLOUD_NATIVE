FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/app.jar /app/app.jar
# JMX Prometheus javaagent: lo activa docker-compose con JAVA_TOOL_OPTIONS (9406 producer, 9407 Streams)
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /app/jmx_prometheus_javaagent.jar
COPY jmx/app.yml /app/jmx-app.yml
CMD ["java", "-cp", "/app/app.jar", "cl.duoc.ventas.StreamsApp"]
