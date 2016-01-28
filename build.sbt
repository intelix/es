Build.settings("es")

lazy val esSinkClient = Project(id = "es-sink-client", base = file("es-sink-client"))
lazy val esSink = Project(id = "es-sink", base = file("es-sink")).dependsOn(esSinkClient).enablePlugins(JavaAppPackaging)
