package com.rahul.spark.sparkcontext

object MostPopularSuperHero extends App {
  val sc = InitializeSpark.getSparkContext("MostPopularSuperHero", "local")

  val names = sc.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\Marvel-names.txt")
  val namesRDD = names.flatMap(line => parseNames(line))
  val lines = sc.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\Marvel-graph.txt")
  val pairing = lines.map(line => countCoOccurences(line))
  val totalFriendsByCharacter = pairing.reduceByKey((x, y) => x + y)
  val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))
  val mostPopular = flipped.max()
  val mostPopularName = namesRDD.lookup(mostPopular._2)(0)

  println(s"$mostPopularName is the most popular hero with ${mostPopular._1} co-appearances")

  def parseNames(line: String): Option[(Int, String)] = {
    val fields = line.split("\"")
    if (fields.length > 1) {
      return Some(fields(0).trim.toInt, fields(1))
    } else {
      return None
    }
  }

  def countCoOccurences(line: String) = {
    val elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }
}
