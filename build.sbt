name := "New_project"

version := "0.2.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.1.1" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "2.1.1" % "provided")

