package com.rahul.spark.loganalyzer

import org.apache.spark.sql.SparkSession

object LogAnalyzerSQL extends App {
  val sparkSession = SparkSession.builder().master("local").getOrCreate()

  import sparkSession.implicits._

  val accessLogs = sparkSession.read.textFile(getClass.getResource("/access.log").getPath).map(x => ApacheAccessLog.parseLogLine(x))
  accessLogs.createOrReplaceTempView("logs")

  // Calculate statistics based on the content size
  val contentSizes = sparkSession.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs").first()

  val minContentSize = contentSizes(2)
  val maxContentSize = contentSizes(3)
  val avgContentSize = contentSizes.getLong(0) / contentSizes.getLong(1)

  println("Min content size: " + minContentSize)
  println("Max content size: " + maxContentSize)
  println("Average content size: " + avgContentSize)

  // Compute Response Code to Count
  val responseCodes = sparkSession.sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 1000").map(row => (row.getInt(0), row.getLong(1)))

  println("Response codes and their counts are follows:")
  responseCodes.collect.sorted.foreach(println)

  // Any IPAddress that has accessed the server more than 10 times
  val ipAddresses = sparkSession.sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 1000").map(row => (row.getString(0), row.getLong(1)))

  println("IP address and their counts are follows:")
  ipAddresses.collect.sorted.foreach(println)

  // Top Endpoints
  val endPoints = sparkSession.sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10").map(row => (row.getString(0), row.getLong(1)))

  println("End Points and their counts are follows:")
  endPoints.collect.foreach(println)
}