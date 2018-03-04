package com.rahul.spark.sparkcontext

object WordCountExample extends App {
  val sc = InitializeSpark.getSparkContext("employee", "local")

    val wordsFileRDD = sc.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/words_input.txt")
  /*
  Array[String] = Array(
  Bus,Car,bus,car,train,car,bus,car,train,bus,TRAIN,BUS,buS,caR,CAR,car,BUS,TRAIN)
   */

  val a = wordsFileRDD.flatMap(w => w.split("\\W+"))
  /*
  Array[String] = Array(
  Bus,
  Car,
  bus,
  car,
  .....
  TRAIN)
  */

  val b = a.countByValue()
  /*
  scala.collection.Map[String,Long] = Map(
  bus -> 3,
  TRAIN -> 2,
  buS -> 1,
  CAR -> 1,
  BUS -> 2,
  car -> 4,
  caR -> 1,
  train -> 2,
  Bus -> 1,
  Car -> 1)
  */

  println(b.mkString("\n"))
}
