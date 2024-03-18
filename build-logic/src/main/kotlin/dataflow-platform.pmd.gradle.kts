import it.polimi.ds.gradle.PmdPreviewArtifactTransform
import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    id("pmd")
}

val pmdRuleSetsFile = file("${rootDir}/config/pmd/rulesSets.xml")
val pmdEnabled = project.ext.has("pmd")

val pmdPreviewAttr = Attribute.of("pmdPreview", Boolean::class.javaObjectType)

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()
dependencies {
    // https://github.com/gradle/gradle/issues/24502
    pmd(libs.bundles.plgns.cq.pmd)
}

pmd {
    ruleSets = listOf()
    toolVersion = libs.versions.pmd.get()
    if(pmdRuleSetsFile.exists()) ruleSetConfig = resources.text.fromFile(pmdRuleSetsFile)
}
// Enable PMD preview features
configurations.pmd { attributes.attribute(pmdPreviewAttr, true) }

tasks.withType<Pmd> {
    enabled = pmdEnabled
    group = "PMD"
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
    isConsoleOutput = true
}

// Register PMD transformer stuff
val artifactType = Attribute.of("artifactType", String::class.java)
dependencies {
    attributesSchema {
        attribute(pmdPreviewAttr)
    }
    artifactTypes.getByName("jar") {
        attributes.attribute(pmdPreviewAttr, false)
    }
    registerTransform(PmdPreviewArtifactTransform::class) {
        from.attribute(pmdPreviewAttr, false).attribute(artifactType, "jar")
        to.attribute(pmdPreviewAttr, true).attribute(artifactType, "jar")
    }
}
