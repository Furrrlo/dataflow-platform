import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    java
}

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()
dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("--enable-preview")
}