plugins {
    application
    id("dataflow-platform.code-quality")
}

dependencies {
    implementation(projects.common)

    testImplementation(testFixtures(projects.common))
}
