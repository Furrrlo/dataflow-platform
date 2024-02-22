import com.github.spotbugs.snom.Confidence
import com.github.spotbugs.snom.Effort
import com.github.spotbugs.snom.SpotBugsTask
import groovy.json.JsonSlurper
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.accessors.dm.LibrariesForLibs
import java.util.regex.Pattern

plugins {
    id("com.github.spotbugs")
    id("net.ltgt.errorprone")
    id("net.ltgt.nullaway")
    id("pmd")
    java
}

val spotBugsFile = file("${rootDir}/config/spotbugs/exclude.xml")
val pmdRuleSetsFile = file("${rootDir}/config/pmd/rulesSets.xml")
val warningsFile = file("${rootDir}/config/javac/warnings.json")
val errorpronePatternsFile = file("${rootDir}/config/errorprone/patterns.json")

val skipErrorprone = project.ext.has("skipErrorprone")
val pmdEnabled = project.ext.has("pmd")
val spotBugsEnabled = project.ext.has("spotbugs")

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()
dependencies {
    // Annotations libraries
    implementation(libs.annotations.jspecify) // Nullability annotations
    implementation(libs.annotations.jetbrains) // IDE annotations
    implementation(libs.annotations.errorprone) // Errorprone annotations for additional checks

    spotbugsPlugins(libs.cq.spotbugsPlugins.sb.contrib)
    spotbugsPlugins(libs.cq.spotbugsPlugins.findsecbugs)

    errorprone(libs.cq.errorprone.core)
    errorprone(libs.cq.errorprone.nullaway)

    // https://github.com/gradle/gradle/issues/24502
    pmd(libs.bundles.plgns.cq.pmd)
}

configurations.all {
    if(!this.name.lowercase().contains("spotbugs")) {
        exclude(group = "com.google.code.findbugs", module = "jsr305") // Will cause problems in Java9+
        exclude(group = "javax.annotation", module = "jsr250-api") // Already in the jre
    }
}

spotbugs {
    effort.set(Effort.MAX)
    reportLevel.set(Confidence.LOW)
    toolVersion.set(libs.versions.spotbugs)
    if(spotBugsFile.exists()) excludeFilter.set(spotBugsFile)
}

pmd {
    ruleSets = listOf()
    toolVersion = libs.versions.pmd.get()
    if(pmdRuleSetsFile.exists()) ruleSetConfig = resources.text.fromFile(pmdRuleSetsFile)
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("-Xmaxerrs", "2000", "-Xmaxwarns", "2000"))

    val objToString: (Any?) -> String? = { json: Any? ->
        if (json is String)
            json
        else if (json is Map<*, *> && json["value"] is String)
            json["value"] as String
        else
            null
    }

    if(warningsFile.exists()) {
        val javacWarnings = JsonSlurper().parseText(warningsFile.readText()) as Map<*, *>
        options.compilerArgs.add("-Xlint:" + listOf(
            (javacWarnings["enable"] as Collection<*>?)?.mapNotNull(objToString) ?: emptyList(),
            (javacWarnings["disable"] as Collection<*>?)?.mapNotNull(objToString)?.map { "-$it" } ?: emptyList()
        ).flatten().joinToString(","))
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
            val isTestTask = name == "compileTestJava"
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

// https://github.com/spotbugs/spotbugs-gradle-plugin/issues/172
val spotbugsPrintReports by tasks.registering {
    enabled = spotBugsEnabled
    doLast {
        if(!spotBugsEnabled)
            return@doLast

        val reportsDir = project.layout.buildDirectory.dir("reports/spotbugs").get().asFile
        reportsDir.listFiles()?.forEach {
            if(!it.name.endsWith(".txt"))
                return@forEach

            println(it.name.substring(0, it.name.length - ".txt".length))
            println(it.readText())
            println()
        }
    }
}

afterEvaluate {
    tasks.withType<SpotBugsTask> {
        enabled = spotBugsEnabled
        group = "SpotBugs"
        extraArgs = listOf("-longBugCodes")
        reports {
            create("text") { required.set(true) }
            create("xml") { required.set(true) }
            create("html") { required.set(true) }
        }
        finalizedBy(spotbugsPrintReports)
    }
}

tasks.withType<Pmd> {
    enabled = pmdEnabled
    group = "PMD"
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
    isConsoleOutput = true
}

