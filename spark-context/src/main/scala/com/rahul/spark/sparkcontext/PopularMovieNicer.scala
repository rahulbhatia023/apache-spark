package com.rahul.spark.sparkcontext

import scala.io.Source

object PopularMovieNicer extends App {
  def loadMovieNames(): Map[Int, String] = {
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("D:\\Softwares\\hadoop-2.8.4\\data\\ml-20m\\movies.csv").getLines()
    lines.foreach(line => {
      val fields = line.split(",")
      if (fields.length > 0) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    })
    movieNames
  }

  val sc = InitializeSpark.getSparkContext("PopularMovie", "local")

  val movieNames = sc.broadcast(loadMovieNames())
  val a = sc.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\ml-20m\\ratings.csv")
  val b = a.map(x => x.split(",")(1))
  val c = b.map(x => (x, 1))
  val d = c.reduceByKey((x, y) => x + y)
  val e = d.map(x => (x._2, x._1))
  val f = e.sortByKey()
  val g = f.map(x => (movieNames.value(x._2.toInt), x._1))

  g.collect().foreach(println)
}
