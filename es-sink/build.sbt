import Build._

settings("es-sink") ++ node ++ launcher ++ sink

libraryDependencies += "commons-io" % "commons-io" % "2.4"
