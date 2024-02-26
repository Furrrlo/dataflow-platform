plugins {
    `java-library`
    id("dataflow-platform.code-quality")
}

dependencies {
    api(libs.nashorn)
    api(libs.slf4j)

    // DB stuff
    api(libs.hikaricp) // Connection pooling
    api(libs.postgresql) // JDBC driver
    api(libs.jooq) // nice DSL
}
