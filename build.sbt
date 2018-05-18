name := "akka-streams"

version := "0.1"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.12",
  "com.spinoco" %% "fs2-mail" % "0.1.1",
  "com.spinoco" %% "fs2-crypto" % "0.2.0"
)