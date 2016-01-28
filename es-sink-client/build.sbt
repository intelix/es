import Build._

settings("es-sink-client") ++ sink

libraryDependencies += "commons-io" % "commons-io" % "2.4"
