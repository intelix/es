Build.settings("es")

lazy val esSink = Project(id = "es-sink", base = file("es-sink")).enablePlugins(JavaAppPackaging)
lazy val esSinkClient = Project(id = "es-sink-client", base = file("es-sink-client"))
