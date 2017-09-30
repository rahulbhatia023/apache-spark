package com.rahul.spark.sparkcontext

object AverageFriendsByAge extends App {
  val sc = InitializeSpark.getSparkContext("AverageFriendsByAge", "local")

  val a = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/friends.csv")
  /*
  Array[String] = Array(
  0,will,33,385,
  1,jean,33,2,
  2,hugh,55,221,
  3,deanna,40,465,
  4,quark,68,21)
   */

  val b = a.map(line => (line.split(",")(2).toInt, line.split(",")(3).toInt))
  /*
  Array[(String, String)] = Array(
  (33,385),
  (33,2),
  (55,221),
  (40,465),
  (68,21))
   */

  val c = b.mapValues(x => (x, 1))
  /*
  Array[(String, (String, Int))] = Array(
  (33,(385,1)),
  (33,(2,1)),
  (55,(221,1)),
  (40,(465,1)),
  (68,(21,1)))
   */

  /* Count sum of friends and number of entries per age */
  val d = c.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  /*
  Array[(String, (String, Int))] = Array(
  (55,(221,1)),
  (40,(465,1)),
  (68,(21,1)),
  (33,(3852,2)))
   */

  /* Compute averages */
  val e = d.mapValues(x => x._1 / x._2)
  /*
  Array[(Int, Int)] = Array(
  (40,465),
  (68,21),
  (55,221),
  (33,193))
   */

  e.collect.sorted.foreach(println)
}
