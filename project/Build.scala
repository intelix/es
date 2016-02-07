import com.typesafe.sbt.digest.Import.digest
import com.typesafe.sbt.gzip.Import.gzip
import com.typesafe.sbt.less.Import.LessKeys
import com.typesafe.sbt.rjs.Import.rjs
import com.typesafe.sbt.web.SbtWeb.autoImport.{Assets, pipelineStages}
import sbt.Keys._
import sbt._

private object Settings {

  val rsVersion = "0.1.2_10-SNAPSHOT"
  val aeronVersion = "0.9"
  val scalaLoggingVersion = "3.1.0"
  val logbackVersion = "1.1.2"
  val akkaStreamVersion = "2.0.2"

  val rsNode = "au.com.intelix" %% "rs-core-node" % rsVersion
  val rsAuth = "au.com.intelix" %% "rs-auth" % rsVersion
  val rsWebsocketServer = "au.com.intelix" %% "rs-websocket-server" % rsVersion
  val rsWebClient = "au.com.intelix" %% "rs-core-js" % rsVersion
  val loggingScala      = "com.typesafe.scala-logging"  %% "scala-logging"                  % scalaLoggingVersion
  val loggingLogback    = "ch.qos.logback"              %  "logback-classic"                % logbackVersion
  val akkaStreams       = "com.typesafe.akka"           %% "akka-stream-experimental"       % akkaStreamVersion

  val aeron = "uk.co.real-logic" % "aeron-all" % aeronVersion


  lazy val baseSettings = Defaults.coreDefaultSettings

  lazy val artifactSettings = Seq(
    version := rsVersion,
    organization := "au.com.intelix",
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    homepage := Some(url("http://reactiveservices.org/"))
  )

  lazy val resolverSettings = Seq(
    resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
    resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    resolvers += "mandubian maven bintray" at "http://dl.bintray.com/mandubian/maven",
    resolvers += "Spray" at "http://repo.spray.io"
  )

  lazy val compilerSettings = Seq(
    scalaVersion := "2.11.7",
    scalacOptions in Compile ++= Seq(
      "-encoding", "UTF-8",
      "-target:jvm-1.8",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlog-reflective-calls",
      "-Xlint",
      "-Xfatal-warnings",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    ),
    incOptions := incOptions.value.withNameHashing(nameHashing = true),
    evictionWarningOptions in update := EvictionWarningOptions
      .default.withWarnTransitiveEvictions(false).withWarnDirectEvictions(false).withWarnScalaVersionEviction(false),
    doc in Compile <<= target.map(_ / "none")
  )


  lazy val testSettings = Seq(
    testOptions in Test += Tests.Argument("-oDF"),
    testListeners in(Test, test) := Seq(TestLogger(streams.value.log, { _ => streams.value.log }, logBuffered.value)),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
  )

  lazy val concurrencySettings = Seq(
    concurrentRestrictions in Global := Seq(
      Tags.limit(Tags.Test, 1),
      Tags.limitAll(1)
    ),
    parallelExecution in Global := false
  )

  lazy val defaultSettings = artifactSettings ++ baseSettings ++ resolverSettings ++ compilerSettings ++ testSettings ++ concurrencySettings
}

object Build {

  import Settings._

  def settings(module: String) = defaultSettings ++: Seq(
    name := module,
    parallelExecution in Global := false,
    doc in Compile <<= target.map(_ / "none"),
    libraryDependencies ++= Seq(loggingScala, loggingLogback)
  )

  lazy val web = Seq(
    includeFilter in(Assets, LessKeys.less) := "*.less",
    excludeFilter in(Assets, LessKeys.less) := "_*.less",
    pipelineStages := Seq(rjs, digest, gzip)
  )

  lazy val launcher = Seq(
    mainClass in Compile := Some("rs.node.Launcher")
  )

  lazy val webClient = Seq(
    libraryDependencies += rsWebClient
  )

  lazy val websocketServer = Seq(
    libraryDependencies += rsWebsocketServer
  )

  lazy val auth = Seq(
    libraryDependencies += rsAuth
  )

  lazy val node = Seq(
    libraryDependencies += rsNode
  )

  lazy val sink = Seq(
    libraryDependencies ++= List(aeron, akkaStreams)
  )


}
