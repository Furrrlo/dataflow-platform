import org.gradle.accessors.dm.LibrariesForLibs
import org.jooq.meta.jaxb.GeneratedAnnotationType
import org.jooq.meta.jaxb.VisibilityModifier

plugins {
    java
    id("org.jooq.jooq-codegen-gradle")
}

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()
dependencies {
    // Add the db itself for its transitive dependencies
    // otherwise it won't have the codegen stuff and won't be able to compile the SinglePackageStrategy
    jooqCodegen(libs.plgns.db.jooq) {
        attributes {
            // Need the transitive dependencies
            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage::class.java, Usage.JAVA_RUNTIME))
        }
    }
    jooqCodegen(libs.jooq.meta.liquibase)
}

jooq {
    configuration {
        // Generate everything package private
        // https://blog.jooq.org/how-to-generate-package-private-code-with-jooqs-code-generator/
        generator {
            strategy {
                // Generates all objects in the same package
                name = "it.polimi.ds.dataflow.SinglePackageStrategy"
                java = """package it.polimi.ds.dataflow;
                         |
                         |import org.jooq.codegen.DefaultGeneratorStrategy;
                         |import org.jooq.codegen.GeneratorStrategy.Mode;
                         |import org.jooq.meta.Definition;
                         |  
                         |public class SinglePackageStrategy extends DefaultGeneratorStrategy {
                         |    @Override
                         |    public String getJavaPackageName(Definition definition, Mode mode) {
                         |        return getTargetPackage();
                         |    }
                         |}"""
                    .trimMargin()
            }
            generate {
                visibilityModifier = VisibilityModifier.NONE

                isGeneratedAnnotation = true
                generatedAnnotationType = GeneratedAnnotationType.ORG_JOOQ_GENERATED
                isGeneratedAnnotationDate = false
                isGeneratedAnnotationJooqVersion = true
            }
            database {
                name = "org.jooq.meta.extensions.liquibase.LiquibaseDatabase"
                properties {
                    property {
                        key = "rootPath"
                        value = "${projectDir}/src/main/resources"
                    }
                    property {
                        key = "scripts"
                        value = "it/polimi/ds/dataflow/worker/dfs/changelog-root.xml"
                    }
                }
            }
            target {
                packageName = "it.polimi.ds.dataflow.worker.dfs"
            }
        }
    }
}

tasks.compileJava {
    dependsOn(tasks.named("jooqCodegen"))
}
