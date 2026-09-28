plugins {
    kotlin("jvm") version "2.4.10"
    application
}

group = "minikafka"
version = "0.1.0"

repositories {
    mavenCentral()
}

configurations.all {
    exclude(group = "ch.qos.logback")
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    implementation("org.apache.curator:curator-framework:5.9.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
    // `minikafka zk` runs ZooKeeperServerMain, which needs these at runtime (same versions
    // curator-test brings for TestingServer): metrics-core for ServerMetrics, and snappy-java
    // because ZooKeeper 3.9's SnapStream loads SnappyOutputStream even with the default
    // (uncompressed) snapshot format — without it the server thread dies with NoClassDefFoundError.
    runtimeOnly("io.dropwizard.metrics:metrics-core:3.2.5")
    runtimeOnly("org.xerial.snappy:snappy-java:1.1.10.4")

    testImplementation("org.apache.curator:curator-test:5.9.0")
}

application {
    mainClass.set("minikafka.cli.CliKt")
}

val excludedTestTags = listOf("e2e", "slow") +
    (project.findProperty("excludeTags") as String? ?: "")
        .split(",")
        .map { it.trim() }
        .filter { it.isNotEmpty() }

tasks.test {
    useJUnitPlatform {
        excludeTags(*excludedTestTags.toTypedArray())
    }
    systemProperty("zookeeper.admin.enableServer", "false")
    systemProperty("zookeeper.sasl.client", "false")
}

val e2eTest = tasks.register<Test>("e2eTest") {
    description = "Runs end-to-end and slow tests against an installed distribution."
    group = "verification"

    testClassesDirs = tasks.test.get().testClassesDirs
    classpath = tasks.test.get().classpath

    useJUnitPlatform {
        includeTags("e2e", "slow")
    }
    maxParallelForks = 1
    dependsOn(tasks.named("installDist"))
    // The nemesis seed is printed by NemesisE2eTest; replay a run with ./gradlew e2eTest -Dnemesis.seed=<seed>.
    providers.systemProperty("nemesis.seed").orNull?.let { systemProperty("nemesis.seed", it) }
    systemProperty(
        "minikafka.home",
        layout.buildDirectory.dir("install/minikafka").get().asFile.absolutePath
    )
    systemProperty("zookeeper.admin.enableServer", "false")
    systemProperty("zookeeper.sasl.client", "false")
}

tasks.check {
    dependsOn(e2eTest)
}
