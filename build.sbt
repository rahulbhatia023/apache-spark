lazy val commonSettings = Seq(
  organization := "com.rahul.spark",
  version := "1.0-SNAPSHOT",
  scalaVersion := "2.11.12"
)

val sparkContext = (project in file("spark-context")).settings(commonSettings)
val sparkSQL = (project in file("spark-sql")).settings(commonSettings)

val apacheSpark = (project in file(".")).settings(commonSettings).aggregate(sparkContext, sparkSQL)
