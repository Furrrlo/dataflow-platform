plugins {
    java
}

dependencies {
    testImplementation(projects.coordinator)
    testImplementation(projects.worker)
    testImplementation(projects.common)
    testImplementation(testFixtures(projects.common))

    testImplementation(libs.slf4j.simple)
}
