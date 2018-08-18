package com.rahul.spark.sparkcontext

object ReduceByKeyExample extends App {
  val sc = InitializeSpark.getSparkContext("employee", "local")

  val rdd1 = sc.parallelize(Array("two", "two", "four", "five", "six", "six", "eight", "nine", "ten"))
  val rdd2 = rdd1.map(w => (w, 1)).reduceByKey((x, y) => x + y)

  rdd2.foreach(println)
}