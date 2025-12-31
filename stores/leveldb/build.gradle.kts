plugins {
    kotlin("jvm")
    `java-library`
    `publishing-convention`
    id("org.jlleitschuh.gradle.ktlint")
    kotlin("plugin.serialization")
}

kotlin {
    jvmToolchain(11)
}

dependencies {
    api(projects.core)
    api(libs.kotlin.leveldb)

    testImplementation(projects.tests)
    testImplementation(kotlin("test"))
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}