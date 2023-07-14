plugins {
  kotlin("jvm") version "1.8.0"
  `maven-publish`
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
  testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test-jvm:1.7.1")
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

publishing {
  repositories {
    mavenCentral()
  }
  publications {
    create<MavenPublication>("mavenJava") {
      pom {
        name.set("kotgresql")
        description.set("A PostgreSQL client library written in pure Kotlin, made for use with Coroutines")
        url.set("https://github.com/BastianVoigt/kotgresql")
        licenses {
          license {
            name.set("The Apache License, Version 2.0")
            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
          }
        }
        developers {
          developer {
            id.set("bvoigt")
            name.set("Bastian Voigt")
            email.set("post@bastian-voigt.de")
          }
        }
        scm {
          connection.set("scm:git:ssh://github.com/BastianVoigt/kotgresql.git")
          developerConnection.set("scm:git:ssh://github.com/BastianVoigt/kotgresql.git")
          url.set("https://github.com/BastianVoigt/kotgresql")
        }
      }
    }
  }
}