package com.rahul.spark.sparksql

import org.apache.spark.sql.SparkSession

object SparkUDF extends App {
  val sparkSession = SparkSession.builder().master("local").getOrCreate()
  val dataFrame = sparkSession.read.format("com.databricks.spark.csv")
    .option("header", "true").option("inferSchema", "true")
    .load("D:\\Softwares\\hadoop-2.8.4\\data\\EMP.csv")

  dataFrame.createOrReplaceTempView("employees")

  val function = (s: String) => s.toUpperCase

  sparkSession.udf.register("PARSE_GENDER", function)

  sparkSession.sql("SELECT PARSE_GENDER(FIRST_NAME), PARSE_GENDER(LAST_NAME) FROM employees").show()
}