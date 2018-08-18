package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext

object IntersectionExample extends App {
  val sc: SparkContext = InitializeSpark.getSparkContext("employee", "local")

  val rdd1 = sc.parallelize(1 to 9)
  val rdd2 = sc.parallelize(5 to 15)

  val rdd3 = rdd1.intersection(rdd2)

  rdd3.collect()
  /*
  Array[Int] = Array(8, 9, 5, 6, 7)
 */
}