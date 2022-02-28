ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Task2Films"
  )

val sparkVersion = "3.2.1"

libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

