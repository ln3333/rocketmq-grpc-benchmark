FROM maven:3.9-eclipse-temurin-8 AS build
WORKDIR /src

COPY pom.xml .
RUN mvn -B -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:8-jre-jammy
WORKDIR /app

RUN groupadd --system benchmark \
    && useradd --system --gid benchmark --home-dir /app benchmark \
    && chown benchmark:benchmark /app
COPY --from=build /src/target/rocketmq-grpc-benchmark-all.jar /app/rocketmq-grpc-benchmark.jar

USER benchmark
ENTRYPOINT ["java", "-jar", "/app/rocketmq-grpc-benchmark.jar"]
CMD ["producer", "--help"]
