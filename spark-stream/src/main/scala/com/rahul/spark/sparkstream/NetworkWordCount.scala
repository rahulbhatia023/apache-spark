package com.rahul.spark.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount extends App {
  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
  val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(1))

  val lines = sparkStreamingContext.socketTextStream("localhost", 4040, StorageLevel.MEMORY_AND_DISK_SER)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

  wordCounts.print()
  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()
}
