plugins {
    versions
    id("maven-publish")
}

subprojects {
    plugins.withType<JavaPlugin> {
        apply(plugin = "maven-publish")

        configure<PublishingExtension> {
            publications {
                create<MavenPublication>("mavenJava") {
                    artifact(tasks.named("jar"))

                    groupId = "com.github.lamba92"
                    artifactId = project.name
                    version = "1.0.0"
                }
            }
        }
    }
}
