plugins {
    java
}

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("--enable-preview")
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs("--enable-preview")
}

tasks.jar {
    // Since stuff is included as a jar in jib containers,
    // we want the jar to be built reproducibly.
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
