package com.rahul.spark.sparkcontext

object DistinctExample extends App {
  val sc = InitializeSpark.getSparkContext("employee", "local")

  val rdd = sc.parallelize(Array(1, 2, 3, 3, 4, 5))
  rdd.distinct().collect
  /*
  res2: Array[Int] = Array(1, 2, 3, 4, 5)
  */
}
