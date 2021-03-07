import sbt._

object Dependencies {
  val config = "com.typesafe" % "config" % "1.4.0"
  val fs2 = "co.fs2" %% "fs2-core" % Versions.fs2
  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val monix = "io.monix" % "monix_2.13" % "3.3.0"
  val mqttClient = "net.sigusr" %% "fs2-mqtt" % "0.4.2"
  val pureconfig = "com.github.pureconfig" %% "pureconfig" % "0.14.0"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"
  val slf4j = "org.slf4j" % "slf4j-api" % Versions.slf4j
  val slf4jJulBridge = "org.slf4j" % "jul-to-slf4j" % Versions.slf4j
  val slf4jJclBridge = "org.slf4j" % "jcl-over-slf4j" % Versions.slf4j
  val slog4sApi = "com.avast" %% "slog4s-api" % Versions.slog4s
  val slog4sSlf4j = "com.avast" %% "slog4s-slf4j" % Versions.slog4s

  object Versions {
    val fs2 = "2.5.0"
    val metrics = "2.7.3"
    val slf4j = "1.7.30"
    val slog4s = "0.6.0"
  }
}
