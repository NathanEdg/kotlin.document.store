@file:Suppress("OPT_IN_USAGE")


plugins {
    kotlin("jvm")
    `publishing-convention`
    id("org.jlleitschuh.gradle.ktlint")
    kotlin("plugin.serialization")
}

kotlin {
    jvmToolchain(11)
}

dependencies {
    api(libs.kotlinx.serialization.json)
    api(libs.kotlinx.coroutines.core)
}
