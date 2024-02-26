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
        testImplementation(theLibs.bundles.testcontainers) {
            exclude(group = theLibs.junit4.map { it.group }.get())
        }

    }

    tasks.withType<JavaCompile>().configureEach {
        options.compilerArgs.add("--enable-preview")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs("--enable-preview")
    }

    tasks.withType<JavaExec>().configureEach {
        jvmArgs("--enable-preview")
    }
}