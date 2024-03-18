import com.github.spotbugs.snom.Confidence
import com.github.spotbugs.snom.Effort
import com.github.spotbugs.snom.SpotBugsTask
import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    id("com.github.spotbugs")
}

val spotBugsFile = file("${rootDir}/config/spotbugs/exclude.xml")
val spotBugsEnabled = project.ext.has("spotbugs")

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()
dependencies {
    spotbugsPlugins(libs.cq.spotbugsPlugins.sb.contrib)
    spotbugsPlugins(libs.cq.spotbugsPlugins.findsecbugs)
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
