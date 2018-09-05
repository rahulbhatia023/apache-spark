package com.rahul.spark.sparkcontext

object CartesianProduct extends App {
  val sc = InitializeSpark.getSparkContext("AverageFriendsByAge", "local")

  val a = sc.parallelize(List(1, 2, 3))
  val b = sc.parallelize(List(4, 5))
  val c = a.cartesian(b)

  c.foreach(println)
}
