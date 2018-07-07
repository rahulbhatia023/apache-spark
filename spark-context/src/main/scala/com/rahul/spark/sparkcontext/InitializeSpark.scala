package com.rahul.spark.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}

object InitializeSpark {
  def getSparkContext(appName: String, master: String): SparkContext =
    new SparkContext(new SparkConf().setAppName(appName).setMaster(master))
}