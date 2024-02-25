rootProject.name = "dataflow-platform"

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

includeBuild("build-logic")

include(":common")
include(":coordinator")
include(":worker")
