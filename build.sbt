Build.settings("es")

lazy val esSinkClient = Project(id = "es-sink-client", base = file("es-sink-client"))
lazy val esSink = Project(id = "es-sink", base = file("es-sink")).dependsOn(esSinkClient).enablePlugins(JavaAppPackaging)
lazy val esSource = Project(id = "es-source", base = file("es-source")).dependsOn(esSinkClient).enablePlugins(JavaAppPackaging)

lazy val esAgentApi = Project(id = "es-agent-api", base = file("es-agent/es-agent-api"))
lazy val esAgentService = Project(id = "es-agent-service", base = file("es-agent/es-agent-service")).dependsOn(esSinkClient, esAgentApi).enablePlugins(JavaAppPackaging)

