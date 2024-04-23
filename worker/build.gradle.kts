plugins {
    application
    id("dataflow-platform.java")
    id("dataflow-platform.test")
    id("dataflow-platform.code-quality")
    id("dataflow-platform.database")
}

dependencies {
    implementation(projects.common)
    implementation(libs.liquibase)

    testImplementation(testFixtures(projects.common))
}
