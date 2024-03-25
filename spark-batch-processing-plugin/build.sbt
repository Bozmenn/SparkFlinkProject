ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "spark-batch-processing-plugin",
    idePackagePrefix := Some("com.berkozmen")
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"

libraryDependencies += "junit" % "junit" % "4.12" % "test"