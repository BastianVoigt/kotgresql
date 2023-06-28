plugins {
  kotlin("jvm") version "1.8.0"
  id("org.jlleitschuh.gradle.ktlint") version "11.4.2"
}

group = "kotgresql"
version = "0.1.0-SNAPSHOT"

repositories {
  mavenCentral()
}

dependencies {
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.7.1")

  testImplementation(kotlin("test"))
  testImplementation("org.hamcrest:hamcrest:2.2")
  testImplementation("org.testcontainers:postgresql:1.18.3")
  testImplementation("org.testcontainers:junit-jupiter:1.18.3")
}

tasks.test {
  useJUnitPlatform()
  testLogging {
    showStandardStreams = true
    showStackTraces = true
    showExceptions = true
    showCauses = true
  }
}

kotlin {
  jvmToolchain(17)
}

configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
  version.set("0.45.2")
}