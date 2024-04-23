plugins {
    application
    id("dataflow-platform.java")
    id("dataflow-platform.test")
    id("dataflow-platform.code-quality")
}

dependencies {
    implementation(projects.common)

    testImplementation(testFixtures(projects.common))
}
