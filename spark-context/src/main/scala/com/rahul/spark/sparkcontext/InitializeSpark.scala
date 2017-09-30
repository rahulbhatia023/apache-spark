package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object InitializeSpark {
  def getSparkContext(appName: String, master: String): SparkContext =
    new SparkContext(new SparkConf().setAppName(appName).setMaster(master))
}