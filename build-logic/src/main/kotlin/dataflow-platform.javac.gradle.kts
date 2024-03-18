import groovy.json.JsonSlurper
import java.util.regex.Pattern

plugins {
    java
}

val warningsFile = file("${rootDir}/config/javac/warnings.json")
val skipWError = project.ext.has("skipErrorprone")

tasks.withType<JavaCompile> {
    val isTestTask = name == "compileTestJava"
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

    if(!isTestTask && !skipWError) {
        // Since -werror is pretty unusable:
        // - inability to suppress some specific types of warnings: https://bugs.openjdk.org/browse/JDK-8305250
        // - errorprone only logs first warning and not all of them: https://github.com/google/error-prone/issues/1341
        // detect warnings in the output manually and fail
        var warnings = 0
        // From https://github.com/jenkinsci/analysis-model/blob/f2409fd3ce56260c9768461f1fbae96d8a2f6792/src/main/java/edu/hm/hafner/analysis/parser/AntJavacParser.java#L23-L26
        @Suppress("RegExpRedundantEscape") // Don't want any difference from where I copied it
        val javacPattern = Pattern
            .compile("^(?:.*\\[[^]]*\\])?\\s*" // Ant task
                    + "\\s*(.*java):(\\d*):\\s*"
                    + "(warning|error|\u8b66\u544a)\\s*:\\s*(?:\\[(\\w*)\\])?\\s*(.*)$"
                    + "|^\\s*\\[.*\\]\\s*warning.*\\]\\s*(.*\"(.*)\".*)$"
                    + "|^(.*class)\\s*:\\s*warning\\s*:\\s*(.*)$"
                    // Next line is added by me, it's the same warning as the line above, but it also specifies the jar
                    // See https://github.com/jenkinsci/analysis-model/blob/f2409fd3ce56260c9768461f1fbae96d8a2f6792/src/test/resources/edu/hm/hafner/analysis/parser/issue21240.txt
                    // aaa.jar(/pkg/aaa.class): warning: Cannot find annotation method 'xxx()' in type 'yyyy'
                    + "|^(:?.*jar\\(.*class\\))\\s*:\\s*warning\\s*:\\s*(.*)$")
        // From https://github.com/jenkinsci/analysis-model/blob/f2409fd3ce56260c9768461f1fbae96d8a2f6792/src/main/java/edu/hm/hafner/analysis/parser/GradleErrorProneParser.java#L22-L27
        @Suppress("RegExpRedundantEscape") // Don't want any difference from where I copied it
        val errorpronePattern = Pattern
            .compile("^(?<file>.+):"
                    + "\\s*(?<line>\\d+)\\s*:"
                    + "\\s*(?<severity>warning|error)\\s*:"
                    + "\\s*\\[(?<type>\\w+)\\]\\s+"
                    + "(?<message>.*)$")
        logging.addStandardErrorListener { str ->
            if(!javacPattern.matcher(str).matches() && !errorpronePattern.matcher(str).matches())
                return@addStandardErrorListener
            // Useless warning, can't be suppressed in any way
            if(str.contains("Cannot find annotation method ") && str.contains(" in type "))
                return@addStandardErrorListener

            warnings++
        }
        doLast {
            if(warnings > 0)
                throw RuntimeException("Found $warnings warnings")
        }
    }
}
