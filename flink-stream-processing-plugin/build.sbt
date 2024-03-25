ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "flink-stream-processing-plugin"
  )

libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.14.2"
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.14.2"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % "1.14.2"


libraryDependencies += "io.circe" %% "circe-core" % "0.14.1"
libraryDependencies += "io.circe" %% "circe-generic" % "0.14.1"
libraryDependencies += "io.circe" %% "circe-parser" % "0.14.1"

libraryDependencies += "junit" % "junit" % "4.12" % "test"

