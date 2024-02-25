plugins {
    `java-library`
    id("dataflow-platform.code-quality")
}

dependencies {
    api(libs.nashorn)
    api(libs.slf4j)

    // Postgres
    //api("org.postgresql:postgresql:42.7.2")
}
