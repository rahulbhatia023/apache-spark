package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext

object DistinctExample extends App {
  val sparkContext: SparkContext = InitializeSpark.getSparkContext("employee", "local")

  val rdd = sparkContext.parallelize(Array(1, 2, 3, 3, 4, 5))
  rdd.distinct().collect
  /*
  res2: Array[Int] = Array(1, 2, 3, 4, 5)
  */
}
