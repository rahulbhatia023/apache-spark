package com.rahul.spark.sparksql

import org.apache.spark.sql.SparkSession

object DataFrameCSV extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  val dataFrame = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("D:\\Softwares\\hadoop-2.8.4\\data\\EMP.csv")

  // Displays the content of the DataFrame
  dataFrame.show()

  // Print the schema in a tree format
  dataFrame.printSchema()

  // Select only the "FIRST_NAME" column
  dataFrame.select("FIRST_NAME").show()

  // This import is needed to use the $-notation
  import spark.implicits._

  // Select everybody, but increment the SALARY by 1
  dataFrame.select($"FIRST_NAME", $"SALARY" + 1).show()

  // Select SALARY > 6000
  dataFrame.filter($"SALARY" > 6000).show()

  // Count employees by DEPARTMENT_ID
  dataFrame.groupBy("DEPARTMENT_ID").count().show()

  // Register the DataFrame as a SQL temporary view
  dataFrame.createOrReplaceTempView("employees")

  spark.sql("SELECT * FROM employees").show()
}