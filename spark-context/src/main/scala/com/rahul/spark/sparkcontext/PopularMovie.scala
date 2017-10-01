package com.rahul.spark.sparkcontext

object PopularMovie extends App {
  val sc = InitializeSpark.getSparkContext("PopularMovie", "local")

  val a = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/ml-20m/ratings.csv")
  val b = a.map(x => x.split(",")(1))
  val c = b.map(x => (x, 1))
  val d = c.reduceByKey((x, y) => x + y)
  val e = d.map(x => (x._2, x._1))
  val f = e.sortByKey()

  f.collect().foreach(println)
}