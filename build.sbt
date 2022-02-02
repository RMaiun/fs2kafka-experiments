ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.6"

lazy val root = (project in file("."))
  .settings(
    name := "fs2kafka-experiments",
    idePackagePrefix := Some("dev.rmaiun")
  )

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "com.github.fd4s"        %% "fs2-kafka"        % "3.0.0-M4",
  "com.github.fd4s"        %% "fs2-kafka-vulcan" % "3.0.0-M4",
  "org.typelevel"          %% "cats-core"        % "2.7.0",
  "org.typelevel"          %% "cats-effect"      % "3.3.4",
  "ch.qos.logback"          % "logback-classic"  % "1.2.10",
  "io.projectreactor.kafka" % "reactor-kafka"    % "1.3.9"
)
