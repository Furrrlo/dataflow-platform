plugins {
    id("dataflow-platform.java")
    id("dataflow-platform.test")
}

dependencies {
    listOf(
        projects.coordinator,
        projects.worker
    ).forEach {
        testImplementation(it) {
            attributes {
                // Also request the runtime dependencies (the ones declared as implementation)
                attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage::class.java, Usage.JAVA_RUNTIME))
            }
        }
    }

    testImplementation(testFixtures(projects.common))
    testImplementation(libs.slf4j.simple)
}
