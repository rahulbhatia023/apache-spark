package com.rahul.spark.sparkcontext

import scala.io.Source

object PracticeExercises extends App {
  val sc = InitializeSpark.getSparkContext("PracticeExercises", "local")

  val users = sc.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\users.csv")
  val tweets = sc.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\tweets.csv")

  // Question - 1a
  val nyUsers = users.map(line => line.split(",")).map(x => (x(0), x(2))).filter(x => x._2.equals("NY"))
  nyUsers.foreach(println)

  // Question - 1b
  import scala.util.matching.Regex

  val a = tweets.map(x => getTweetParameters(x))
  val b = a.filter(x => x._2.contains("favorite"))
  b.foreach(println)

  def getTweetParameters(x: String): (String, String, String) = {
    val keyValPattern: Regex = "(.*),\"(.*)\",(.*)".r
    var a: (String, String, String) = null
    keyValPattern.findAllIn(x).matchData foreach {
      patternMatch => {
        a = (patternMatch.group(1), patternMatch.group(2), patternMatch.group(3))
      }
    }
    a
  }

  // Question - 2b
  val u = users.map(x => x.split(",")).map(x => (x(0), x))
  val t = tweets.map(x => (getTweetParameters(x)._3, x))
  val usersDictionary = loadUserNames()

  val join = u.join(t)
  join.foreach(println)

  // Question - 3a
  val c = t.countByKey()
  val d = c.map(x => (usersDictionary(x._1), x._2))
  d.foreach(println)

  // Question - 3b
  val e = d.toSeq.sortWith(_._2 > _._2)
  e.foreach(println)

  def loadUserNames() = {
    var userNames: Map[String, String] = Map()
    val lines = Source.fromFile("D:\\Softwares\\hadoop-2.8.4\\data\\users.csv").getLines()
    lines.foreach(line => {
      val fields = line.split(",")
      if (fields.length > 1) {
        userNames += (fields(0) -> fields(1))
      }
    })
    userNames
  }

  // Question - 4a
  val f = d.filter(_._2 >= 2)
  f.foreach(println)

  // Question - 4b
  val g = u.leftOuterJoin(t)
  val h = g.filter(x => x._2._2.isEmpty)
  h.foreach(x => println(x._1))
}