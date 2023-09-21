ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "luxoft-scala-task",
    libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
)
