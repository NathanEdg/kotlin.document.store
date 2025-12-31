@file:Suppress("UnstableApiUsage")

import java.nio.file.Path
import kotlin.io.path.absolutePathString
import kotlin.io.path.exists
import kotlin.io.path.isDirectory

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
    id("com.gradle.develocity") version "4.0.2"
}

rootProject.name = "kotlin-document-store"
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

// -----------------------------
// Module Includes
// -----------------------------
include(
    ":core",
    ":stores:leveldb",
    ":tests",
    ":version-catalog"
)

// -----------------------------
// Repositories for dependency resolution
// -----------------------------
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
    rulesMode = RulesMode.PREFER_SETTINGS
}

// -----------------------------
// Optional: include local LevelDB build if it exists
// -----------------------------
val levelDbPath: Path = file("../kotlin-leveldb").toPath()
val localLeveldbExists = levelDbPath.isDirectory() && levelDbPath.resolve("settings.gradle.kts").exists()
val useLocalLevelDb: Boolean? by settings

val isCi: Boolean
    get() = System.getenv("CI") == "true"

if (localLeveldbExists && !isCi && useLocalLevelDb == true) {
    includeBuild(levelDbPath.absolutePathString()) {
        dependencySubstitution {
            // Only substitute JVM artifact
            substitute(module("com.github.lamba92:kotlin-leveldb")).using(project(":"))
        }
    }
}

// -----------------------------
// Develocity Build Scan Configuration
// -----------------------------
develocity {
    buildScan {
        termsOfUseUrl = "https://gradle.com/terms-of-service"
        termsOfUseAgree = "yes"
        publishing {
            onlyIf { isCi }
        }
    }
}
