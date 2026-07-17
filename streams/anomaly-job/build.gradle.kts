import com.github.davidmc24.gradle.plugin.avro.GenerateAvroJavaTask

plugins {
    java
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
    // Confluent serdes (kafka-streams-avro-serde) are published only to the Confluent repo.
    maven("https://packages.confluent.io/maven/")
}

val kafkaVersion = "3.6.1"
val confluentVersion = "7.6.1"
val avroVersion = "1.11.3"
val testcontainersVersion = "1.20.6"
val junitVersion = "5.10.2"

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("io.confluent:kafka-streams-avro-serde:$confluentVersion")
    implementation("org.apache.avro:avro:$avroVersion")
    // Emit startup logs (Kafka Streams logs the "State transition ... RUNNING" line via SLF4J).
    runtimeOnly("org.slf4j:slf4j-simple:2.0.13")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    testImplementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    testImplementation("org.testcontainers:redpanda:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    // Testcontainers' tar streaming (commons-compress 1.26) references commons-codec's Charsets,
    // which is not otherwise on the classpath — pin a version that still provides it.
    testImplementation("commons-codec:commons-codec:1.16.1")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

avro {
    stringType.set("String")
    fieldVisibility.set("PRIVATE")
    outputCharacterEncoding.set("UTF-8")
}

// Generate SpecificRecord classes from an EXPLICIT two-file list of the repo-root v1 schemas.
// Never glob schemas/: once M08 adds vehicle-event.v2.avsc, directory-wide codegen would
// redefine the same VehicleEvent class and collide. rootDir is the `streams` root project,
// so ../schemas resolves to the repo-root schemas directory (files are NOT copied in).
tasks.named<GenerateAvroJavaTask>("generateAvroJava") {
    setSource(
        files(
            "$rootDir/../schemas/vehicle-event.v1.avsc",
            "$rootDir/../schemas/anomaly.v1.avsc",
        ),
    )
}

application {
    mainClass.set("com.gridpulse.anomaly.AnomalyJob")
}

tasks.test {
    useJUnitPlatform()
    // The integration test registers the CANONICAL repo-root AVSC text (not generated class schemas)
    // so it exercises the real production schema-identity path. Point it at the same repo-root schemas
    // the Avro plugin reads.
    systemProperty("gridpulse.schemas.dir", "$rootDir/../schemas")
    // Docker Engine >= 29 raised its minimum API version above docker-java's default probe version,
    // which makes Testcontainers' client discovery fail with HTTP 400 on some local Docker Desktop
    // setups. Exporting DOCKER_API_VERSION pins docker-java (via its `api.version` system property)
    // to a supported version for the forked test JVM. Inert in CI, where the env var is unset.
    System.getenv("DOCKER_API_VERSION")?.let { systemProperty("api.version", it) }
}
