package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext

object WordCountExample extends App {
  val sparkContext: SparkContext = InitializeSpark.getSparkContext("employee", "local")

  val wordsFileRDD = sparkContext.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/words_input.txt")
  /*
  Array[String] = Array(
  Bus,Car,bus,car,train,car,bus,car,train,bus,TRAIN,BUS,buS,caR,CAR,car,BUS,TRAIN)
   */

  val a = wordsFileRDD.flatMap(w => w.split(","))
  /*
  Array[String] = Array(
  Bus,
  Car,
  bus,
  car,
  .....
  TRAIN)
  */

  val b = a.map(w => (w, 1))
  /*
  Array[(String, Int)] = Array(
  (Bus,1),
  (Car,1),
  (bus,1),
  (car,1),
  .....
  (TRAIN,1))
   */

  val c = b.reduceByKey((x, y) => x + y)
  /*
  Array[(String, Int)] = Array(
  (TRAIN,2),
  (Bus,1),
  (train,2),
  (Car,1),
  (buS,1),
  (car,4),
  (bus,3),
  (BUS,2),
  (CAR,1),
  (caR,1))
   */
}
