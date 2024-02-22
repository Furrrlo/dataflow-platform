plugins {
    id("java")
    id("dataflow-platform.code-quality")
}

group = "it.polimi.ds"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.nashorn)

    // Logging stuff
    implementation(libs.slf4j)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
}

tasks.test {
    useJUnitPlatform()
}