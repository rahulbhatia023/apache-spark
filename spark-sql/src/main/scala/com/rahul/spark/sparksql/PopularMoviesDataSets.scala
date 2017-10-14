package com.rahul.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source

object PopularMoviesDataSets extends App {

  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: String)

  val sparkSession = SparkSession.builder().appName("PopularMoviesDataSets").master("local").getOrCreate()

  // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
  val lines = sparkSession.sparkContext.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/ml-20m/ratings.csv").map(x => Movie(x.split(",")(1)))

  // Convert to a DataSet
  import sparkSession.implicits._

  val moviesDS = lines.toDS()

  val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count")).cache()

  val top10 = topMovieIDs.take(10)

  val movieNames = loadMovieNames()

  top10.foreach(movie => {
    println(movieNames(movie(0).asInstanceOf[String]) + ": " + movie(1))
  })

  def loadMovieNames(): Map[String, String] = {
    var movieNames: Map[String, String] = Map()
    val lines = Source.fromFile("/home/rahul/Softwares/hadoop-2.8.0/data/ml-20m/movies.csv").getLines()
    lines.foreach(line => {
      val fields = line.split(",")
      if (fields.nonEmpty) {
        movieNames += (fields(0) -> fields(1))
      }
    })
    movieNames
  }
}
