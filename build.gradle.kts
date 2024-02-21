plugins {
    id("java")
}

group = "it.polimi.ds"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.openjdk.nashorn:nashorn-core:15.4")

    // Logging stuff
    implementation("org.slf4j:slf4j-api:2.0.12")

    // Annotations libraries, these do not add any actual functionality, only allow for additional compiler checks
    implementation("org.jspecify:jspecify:0.3.0") // Nullability annotations
    implementation("org.jetbrains:annotations:24.1.0") // IDE annotations
    implementation("com.google.errorprone:error_prone_annotations:2.25.0") // Errorprone annotations for additional checks

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}