plugins {
    application
    id("dataflow-platform.code-quality")
    id("dataflow-platform.database")
}

dependencies {
    implementation(projects.common)
    implementation(libs.liquibase)
    implementation(libs.jasypt)

    testImplementation(testFixtures(projects.common))
}
