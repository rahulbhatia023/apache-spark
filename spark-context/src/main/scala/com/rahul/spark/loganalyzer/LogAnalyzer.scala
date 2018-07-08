package com.rahul.spark.loganalyzer

import com.rahul.spark.sparkcontext.InitializeSpark

object LogAnalyzer extends App {
  val sc = InitializeSpark.getSparkContext("LogAnalyzer", "local")
  val logLines = sc.textFile(getClass.getResource("/access.log").getPath)
  val accessLogs = logLines.map(x => ApacheAccessLog.parseLogLine(x)).cache()

  // Calculate statistics based on the content size.
  val contentSizes = accessLogs.map(x => x.contentSize).cache()

  val minContentSize = contentSizes.min
  val maxContentSize = contentSizes.max
  val avgContentSize = contentSizes.reduce(_ + _) / contentSizes.count

  println("Min content size: " + minContentSize)
  println("Max content size: " + maxContentSize)
  println("Average content size: " + avgContentSize)

  // Compute Response Code to Count
  val responseCodes = accessLogs.map(x => x.responseCode).map(x => (x, 1)).reduceByKey(_ + _)

  println("Response codes and their counts are follows:")
  responseCodes.collect.sorted.foreach(println)

  // Any IPAddress that has accessed the server more than 10 times
  val ipAddresses = accessLogs.map(_.ipAddress).map(x => (x, 1)).reduceByKey(_ + _).filter(_._2 > 10)

  println("IP address and their counts are follows:")
  ipAddresses.collect.sorted.foreach(println)

  // Top Endpoints
  val endPoints = accessLogs.map(_.endpoint).map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._2, false)

  println("End Points and their counts are follows:")
  endPoints.foreach(println)
}