import Build._

settings("es-source") ++ node ++ launcher

libraryDependencies += "commons-io" % "commons-io" % "2.4"
