package com.rahul.spark.loganalyzer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogAnalyzerStreaming extends App {
  val WINDOW_LENGTH = Seconds(30)
  val SLIDE_INTERVAL = Seconds(10)

  val sparkConf = new SparkConf().setAppName("LogAnalyzerStreaming").setMaster("local")
  val streamingContext = new StreamingContext(sparkConf, SLIDE_INTERVAL)

  val logLinesDStream = streamingContext.socketTextStream("localhost", 56565 )
  val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
  val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

  windowDStream.foreachRDD(accessLogs => {
    if (accessLogs.count() == 0) {
      println("No access logs received in this time interval")
    } else {
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
  })

  // Start the streaming server
  streamingContext.start()
  streamingContext.awaitTermination()
}
