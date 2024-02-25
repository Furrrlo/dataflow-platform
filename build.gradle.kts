group = "it.polimi.ds"
version = "1.0-SNAPSHOT"

val theLibs = project.libs
subprojects {
    apply {
        plugin<JavaPlugin>()
    }

    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
    }

    val testImplementation by configurations
    dependencies {
        testImplementation(platform(theLibs.junit.bom))
        testImplementation(theLibs.junit.jupiter)
    }

    tasks.named<Test>("test") {
        useJUnitPlatform()
    }
}