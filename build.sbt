lazy val commonSettings = Seq(
  organization := "com.rahul.spark",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.11"
)

lazy val sparkSQL = (project in file("spark-sql")).settings(
  commonSettings,
  name := "spark-sql",
  libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
)

lazy val apacheSpark = (project in file(".")).settings(
  commonSettings,
  name := "apache-spark"
).aggregate(sparkSQL)