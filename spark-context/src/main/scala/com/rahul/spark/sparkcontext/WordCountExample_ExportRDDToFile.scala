package com.rahul.spark.sparkcontext

object WordCountExample_ExportRDDToFile extends App {
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

  val d = c.map(x => (x._2, x._1))
  /*
  Array[(Int, String)] = Array(
  (2,TRAIN),
  (1,Bus),
  (2,train),
  (1,Car),
  (1,buS),
  (4,car),
  (3,bus),
  (2,BUS),
  (1,CAR),
  (1,caR))
   */

  val e = d.sortByKey()
  /*
  Array[(Int, String)] = Array(
  (1,Bus),
  (1,Car),
  (1,buS),
  (1,CAR),
  (1,caR),
  (2,TRAIN),
  (2,train),
  (2,BUS),
  (3,bus),
  (4,car))
   */

  e.saveAsTextFile("D:\\Softwares\\hadoop-2.8.4\\data\\output\\")
}
