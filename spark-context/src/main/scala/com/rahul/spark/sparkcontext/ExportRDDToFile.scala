package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext

object ExportRDDToFile extends App {
  val sparkContext: SparkContext = InitializeSpark.getSparkContext("employee", "local")

  val wordsFileRDD = sparkContext.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/words_input.txt")

  val a = wordsFileRDD.flatMap(w => w.split(","))

  val b = a.map(w => (w, 1))

  val c = b.reduceByKey(_ + _)

  c.saveAsTextFile("/home/rahul/Softwares/hadoop-2.8.0/data/output/")
}
