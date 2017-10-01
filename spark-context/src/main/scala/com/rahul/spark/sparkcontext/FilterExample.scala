package com.rahul.spark.sparkcontext

object FilterExample extends App {
  val sc = InitializeSpark.getSparkContext("FilterExample", "local")

  val a = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/weather.csv")
  /*
  Array[String] = Array(
  station1,18000101,TMAX,-75,,,E,,
  station1,18000101,TMIN,-148,,,E,,
  station2,18000101,PRCP,0,,,E,,
  station3,18000101,TMAX,-86,,,E,,
  station3,18000101,TMIN,-135,,,E,)
   */

  val b = a.map(x => (x.split(",")(0), x.split(",")(2), x.split(",")(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f))
  /*
  Array[(String, String, Float)] = Array(
  (station1,TMAX,18.5),
  (station1,TMIN,5.3600006),
  (station2,PRCP,32.0),
  (station3,TMAX,16.52),
  (station3,TMIN,7.700001))
   */

  val c = b.filter(x => x._2 == "TMIN")
  /*
  Array[(String, String, Float)] = Array(
  (station1,TMIN,5.3600006),
  (station3,TMIN,7.700001))
   */

  val d = c.map(x => (x._1, x._3.toFloat))
  /*
  Array[(String, Float)] = Array(
  (station1,5.3600006),
  (station3,7.700001))
   */

  val e = d.reduceByKey((x, y) => min(x, y))
  /*
  Array[(String, Float)] = Array(
  (station3,7.700001),
  (station1,5.3600006))
   */

  def min(x: Float, y: Float) = if (x < y) x else y
}