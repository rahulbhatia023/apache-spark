package com.rahul.spark.sparkcontext

object TopViewedProducts extends App {
  val sc = InitializeSpark.getSparkContext("TopViewedProducts", "local")

  val productLogRDD = sc.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\product_log.txt")

  val a = productLogRDD.map(line => (line.split(";")(1).toInt, line.split(";")(2))).filter(x => x._2.equals("Browse") || x._2.equals("Click"))

  val b = a.groupByKey()

  var list: List[(Int, Int, Int)] = List()
  b.foreach(x => {
    var countBrowse: Int = 0
    var countClick: Int = 0
    x._2.foreach(action => {
      if (action.equals("Browse")) countBrowse = countBrowse + 1
      else countClick = countClick + 1
    })
    list = list :+ ((x._1, countBrowse, countClick))
  })

  val c = sc.parallelize(list)

  val d = c.map(x => (x._1, x._2, x._3, x._2 + x._3)).sortBy(_._4, false).take(10)

  val e = sc.parallelize(d).map(x => (x._1, x._2, x._3))

  e.saveAsTextFile("output.txt")
}

