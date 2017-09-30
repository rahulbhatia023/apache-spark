package com.rahul.spark.sparkcontext

object RatingsCounter extends App {
  val sc = InitializeSpark.getSparkContext("RatingsCounter", "local")

  val a = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/ml-20m/ratings.csv")
  val b = a.map(line => line.split(",")(2))
  val c = b.countByValue()
  val d = c.toSeq.sortBy(_._1)

  d.foreach(println)
}