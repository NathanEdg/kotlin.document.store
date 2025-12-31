plugins {
    kotlin("jvm")
    `java-library`
    `publishing-convention`
    id("org.jlleitschuh.gradle.ktlint")
    kotlin("plugin.serialization")
}

kotlin {
    jvmToolchain(11) // optional, matches your core module
}

dependencies {
    implementation(projects.core)
    api(kotlin("test-junit5"))
    implementation(libs.kotlinx.coroutines.test)
    implementation(libs.kotlinx.io.core)

    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}
