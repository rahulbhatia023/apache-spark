package com.rahul.spark.sparkcontext

import scala.io.Source
import scala.math.sqrt

object MovieSimilarities extends App {
  val sc = InitializeSpark.getSparkContext("MovieSimilarities", "local")

  def loadMovieNames(): Map[Int, String] = {
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("/home/rahul/Softwares/hadoop-2.8.0/data/ml-20m/movies.csv").getLines()
    lines.foreach(line => {
      val fields = line.split(",")
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    })
    movieNames
  }

  type UserRatingPair = (Int, ((Int, Double), (Int, Double)))

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val movie1 = userRatings._2._1._1
    val movie2 = userRatings._2._2._1

    movie1 != movie2
  }

  def makePairs(userRatings: UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  val nameDict = loadMovieNames()
  val data = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/ml-20m/ratings.csv")

  // Map ratings to key / value pairs: user ID => movie ID, rating
  val ratings = data.map(l => l.split(",")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

  // Emit every movie rated together by the same user. Self-join to find every combination.
  val joinedRatings = ratings.join(ratings)

  // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating)). Filter out duplicate pairs
  val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

  // Now key by (movie1, movie2) pairs.
  val moviePairs = uniqueJoinedRatings.map(makePairs)

  // We now have (movie1, movie2) => (rating1, rating2). Now collect all ratings for each movie pair and compute similarity
  val moviePairRatings = moviePairs.groupByKey()

  // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2). Can now compute similarities.
  val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

  if (args.length > 0) {
    val scoreThreshold = 0.97
    val coOccurenceThreshold = 50.0

    val movieID: Int = args(0).toInt

    // Filter for movies with this sim that are "good" as defined by our quality thresholds above
    val filteredResults = moviePairSimilarities.filter(x => {
      val pair = x._1
      val sim = x._2
      (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
    }
    )

    // Sort by quality score
    val results = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)

    println("\nTop 10 similar movies for " + nameDict(movieID))
    for (result <- results) {
      val sim = result._1
      val pair = result._2
      // Display the similarity result that isn't the movie we're looking at
      var similarMovieID = pair._1
      if (similarMovieID == movieID) {
        similarMovieID = pair._2
      }
      println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
    }
  }
}
