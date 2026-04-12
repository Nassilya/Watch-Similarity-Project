name := "watches-similarity-project"
version := "1.0"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
  "io.spray" %% "spray-json" % "1.3.6",
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
)