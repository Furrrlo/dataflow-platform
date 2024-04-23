plugins {
    application
    id("dataflow-platform.code-quality")
    id("dataflow-platform.database")
}

dependencies {
    implementation(projects.common)
    implementation(libs.liquibase)

    testImplementation(testFixtures(projects.common))
}
