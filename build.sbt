Build.settings("es")

lazy val reactiveFxPricesource = Project(id = "es-sink", base = file("es-sink")).enablePlugins(JavaAppPackaging)
