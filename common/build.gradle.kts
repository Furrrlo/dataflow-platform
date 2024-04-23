plugins {
    `java-library`
    `java-test-fixtures`
    id("dataflow-platform.java")
    id("dataflow-platform.test")
    id("dataflow-platform.code-quality")
}

dependencies {
    api(libs.nashorn)
    api(libs.slf4j.api)
    implementation(libs.jasypt)
    implementation(libs.jackson)

    // DB stuff
    api(libs.hikaricp) // Connection pooling
    api(libs.postgresql) // JDBC driver
    api(libs.jooq) // nice DSL

    testFixturesApi(libs.bundles.testcontainers) {
        exclude(group = libs.junit4.map { it.group }.get())
    }
    testImplementation(libs.slf4j.simple)
}
