plugins {
    application
    id("dataflow-platform.code-quality")
}

dependencies {
    implementation(projects.common)
    implementation(libs.jasypt)

    testImplementation(testFixtures(projects.common))
}
