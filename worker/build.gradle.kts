plugins {
    application
    id("dataflow-platform.java")
    id("dataflow-platform.test")
    id("dataflow-platform.code-quality")
    id("dataflow-platform.database")
    alias(libs.plugins.jib)
}

dependencies {
    implementation(projects.common)
    implementation(libs.logback.classic)
    implementation(libs.liquibase)

    testImplementation(testFixtures(projects.common))
}

application {
    mainClass = "it.polimi.ds.dataflow.worker.WorkerMain"
}

jib {
    from {
        image = "eclipse-temurin:21" // jre does not contain a bunch of stuff (ex random number generator)
    }
    to {
        image = "our-dataflow-worker"
    }
    container {
        jvmFlags = listOf("--enable-preview")
    }
}
