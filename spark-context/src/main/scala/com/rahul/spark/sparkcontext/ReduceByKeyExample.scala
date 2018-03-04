package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext

object ReduceByKeyExample extends App {
  val sparkContext: SparkContext = InitializeSpark.getSparkContext("employee", "local")
  val rdd1 = sparkContext.parallelize(Array("two", "two", "four", "five", "six", "six", "eight", "nine", "ten"))
  val rdd2 = rdd1.map(w => (w, 1)).reduceByKey(_ + _)

  rdd2.foreach(println)
}
