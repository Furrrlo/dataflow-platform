plugins {
    application
    id("dataflow-platform.java")
    id("dataflow-platform.test")
    id("dataflow-platform.code-quality")
    alias(libs.plugins.jib)
}

dependencies {
    implementation(projects.common)

    testImplementation(testFixtures(projects.common))
}

application {
    mainClass = "it.polimi.ds.dataflow.coordinator.CoordinatorMain"
}

jib {
    from {
        image = "eclipse-temurin:21" // jre does not contain a bunch of stuff (ex random number generator)
    }
    to {
        image = "our-dataflow-coordinator"
    }
    container {
        jvmFlags = listOf("--enable-preview")
    }
}
