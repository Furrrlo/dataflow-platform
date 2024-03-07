plugins {
    `kotlin-dsl`
}

group = "it.polimi.ds.gradle"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation(gradleApi())

    implementation(libs.plgns.cq.spotbugs)
    implementation(libs.plgns.cq.errorprone)
    implementation(libs.plgns.cq.nullaway)
    implementation(libs.bundles.plgns.cq.pmd.asm)

    implementation(libs.plgns.db.jooq) {
        attributes {
            // Need the transitive dependencies on the classpath to compile
            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage::class.java, Usage.JAVA_RUNTIME))
        }
    }

    // https://github.com/gradle/gradle/issues/15383
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
