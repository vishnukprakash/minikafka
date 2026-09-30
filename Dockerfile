# Multi-stage build: compile the minikafka distribution with the Gradle wrapper, then ship only
# the installed distribution on a JRE.

FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
# Keep the build within small Docker VMs (e.g. Colima's 2 GiB default): bounded heaps, one
# worker, and the Kotlin compiler in-process instead of a separate daemon JVM.
ENV GRADLE_OPTS="-Xmx128m"
RUN mkdir -p /root/.gradle && printf '%s\n' \
    'org.gradle.jvmargs=-Xmx768m -XX:MaxMetaspaceSize=384m' \
    'org.gradle.workers.max=1' \
    'org.gradle.daemon=false' \
    'kotlin.compiler.execution.strategy=in-process' \
    > /root/.gradle/gradle.properties
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
# Resolve Gradle and dependencies in their own layer so source edits don't re-download them.
RUN ./gradlew --no-daemon dependencies --configuration runtimeClasspath
COPY src ./src
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:25-jre
RUN useradd --system --uid 1001 --create-home minikafka \
    && mkdir -p /data \
    && chown minikafka /data
COPY --from=build /src/build/install/minikafka /opt/minikafka
COPY docker/minikafka-cli /opt/minikafka/bin/minikafka-cli
ENV PATH="/opt/minikafka/bin:${PATH}" \
    JAVA_OPTS="-Xmx256m --enable-native-access=ALL-UNNAMED"
USER minikafka
WORKDIR /home/minikafka
ENTRYPOINT ["minikafka"]
CMD ["--help"]
