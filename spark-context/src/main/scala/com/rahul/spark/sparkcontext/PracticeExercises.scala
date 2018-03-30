package com.rahul.spark.sparkcontext

object PracticeExercises extends App {
  val sc = InitializeSpark.getSparkContext("PracticeExercises", "local")

  val users = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/users.csv")
  val tweets = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/tweets.csv")

  // Question - 1a
  val nyUsers = users.map(line => line.split(",")).map(x => (x(0), x(2))).filter(x => x._2.equals("NY"))
  nyUsers.foreach(println)

  // Question - 1b
  import scala.util.matching.Regex

  val keyValPattern: Regex = "(.*),\"(.*)\",(.*)".r

  val a = tweets.map(x => getTweetParameters(x))
  val b = a.filter(x => x._2.contains("favorite"))
  b.foreach(println)

  def getTweetParameters(x: String): (String, String, String) = {
    var a: (String, String, String) = null
    keyValPattern.findAllIn(x).matchData foreach {
      patternMatch => {
        a = (patternMatch.group(1), patternMatch.group(2), patternMatch.group(3))
      }
    }
    return a
  }

  // Question - 2b
  val u = users.map(x => x.split(",")).map(x => (x(0), x))
  val t = tweets.map(x => (getTweetParameters(x)._3, x))

  val join = u.join(t)
  join.foreach(println)
}
