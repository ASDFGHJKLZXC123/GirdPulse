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
    runtimeOnly("org.slf4j:slf4j-simple:2.0.13")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    testImplementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    testImplementation("org.testcontainers:redpanda:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("commons-codec:commons-codec:1.16.1")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

avro {
    stringType.set("String")
    fieldVisibility.set("PRIVATE")
    outputCharacterEncoding.set("UTF-8")
}

// Generate SpecificRecord classes from exactly the two repo-root v1 schemas this reader owns.
// Never glob schemas/: M08 adds a v2 VehicleEvent with the same generated class name.
tasks.named<GenerateAvroJavaTask>("generateAvroJava") {
    setSource(
        files(
            "$rootDir/../schemas/vehicle-event.v1.avsc",
            "$rootDir/../schemas/region-rollup.v1.avsc",
        ),
    )
}

application {
    mainClass.set("com.gridpulse.rollup.RollupJob")
}

tasks.test {
    useJUnitPlatform()
    systemProperty("gridpulse.schemas.dir", "$rootDir/../schemas")
    System.getenv("DOCKER_API_VERSION")?.let { systemProperty("api.version", it) }
}
