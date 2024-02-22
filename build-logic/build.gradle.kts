plugins {
    `kotlin-dsl`
}

group = "it.polimi.ds.gradle"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation(libs.plgns.cq.spotbugs)
    implementation(libs.plgns.cq.errorprone)
    implementation(libs.plgns.cq.nullaway)

    // https://github.com/gradle/gradle/issues/15383
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
