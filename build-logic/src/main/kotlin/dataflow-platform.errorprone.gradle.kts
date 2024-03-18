import groovy.json.JsonSlurper
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.accessors.dm.LibrariesForLibs
import java.util.regex.Pattern

plugins {
    id("net.ltgt.errorprone")
    id("net.ltgt.nullaway")
    java
}

val errorpronePatternsFile = file("${rootDir}/config/errorprone/patterns.json")
val skipErrorprone = project.ext.has("skipErrorprone")

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()
dependencies {
    // Annotations libraries
    implementation(libs.annotations.jspecify) // Nullability annotations
    implementation(libs.annotations.jetbrains) // IDE annotations
    implementation(libs.annotations.errorprone) // Errorprone annotations for additional checks

    errorprone(libs.cq.errorprone.core)
    errorprone(libs.cq.errorprone.nullaway)
}

tasks.withType<JavaCompile> {
    val isTestTask = name == "compileTestJava"

    val objToString: (Any?) -> String? = { json: Any? ->
        if (json is String)
            json
        else if (json is Map<*, *> && json["value"] is String)
            json["value"] as String
        else
            null
    }

    options.errorprone {
        isEnabled.set(!skipErrorprone)
        // Use a provider, as options.generatedSourceOutputDirectory causes the compile task to depend on itself
        excludedPaths.set(provider {
            listOfNotNull(
                options.generatedSourceOutputDirectory.asFile.orNull
            ).flatMap { listOf(
                // I have no clue which one should actually work, the Windows one doesn't,
                // and I can't seem to debug it, so I'm just going to shove in both *nix and Windows paths
                ".*${Pattern.quote((File.separator + project.relativePath(it) + File.separator).replace(File.separator, "/"))}.*",
                ".*${Pattern.quote((File.separator + project.relativePath(it) + File.separator).replace(File.separator, "\\"))}.*"
            ) }.joinToString("|", "(?:", ")")
        })

        if(errorpronePatternsFile.exists()) {
            val patterns = JsonSlurper().parseText(errorpronePatternsFile.readText()) as Map<*, *>
            // Patterns disabled in tests, 'cause it's not production code so some don't make sense
            val patternsDisabledInTests =
                (patterns["disableInTests"] as Collection<*>?)?.mapNotNull(objToString)?.toSet() ?: emptySet()
            val filterPatterns: (String) -> Boolean = { s -> !isTestTask || !patternsDisabledInTests.contains(s) }

            (patterns["enable"] as Collection<*>?)?.mapNotNull(objToString)?.filter(filterPatterns)?.forEach { enable(it) }
            (patterns["disable"] as Collection<*>?)?.mapNotNull(objToString)?.filter(filterPatterns)?.forEach { disable(it) }
            (patterns["error"] as Collection<*>?)?.mapNotNull(objToString)?.filter(filterPatterns)?.forEach { error(it) }
            // Disable the checks if in test
            if (isTestTask) patternsDisabledInTests.forEach { disable(it) }
        }

        nullaway {
            annotatedPackages.add("it.polimi.ds")
            checkOptionalEmptiness.set(true)
            suggestSuppressions.set(true)
            checkContracts.set(true)
        }
    }
}
