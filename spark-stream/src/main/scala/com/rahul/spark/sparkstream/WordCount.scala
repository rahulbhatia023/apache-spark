package com.rahul.spark.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount extends App {
  val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
  val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(10))

  val lines = sparkStreamingContext.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)

  wordCounts.print()

  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()
}
