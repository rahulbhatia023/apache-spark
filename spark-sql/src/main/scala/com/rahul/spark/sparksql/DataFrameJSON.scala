package com.rahul.spark.sparksql

import org.apache.spark.sql.SparkSession

object DataFrameJSON extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  val dataFrame = spark.read.json("D:\\Softwares\\hadoop-2.8.4\\data\\first_table.json")

  // Displays the content of the DataFrame
  dataFrame.show()

  // Print the schema in a tree format
  dataFrame.printSchema()

  // Select only the "person" column
  dataFrame.select("person").show()

  // This import is needed to use the $-notation
  import spark.implicits._

  // Select everybody, but increment the amount by 1
  dataFrame.select($"person", $"amount" + 1).show()

  // Select amount > 2
  dataFrame.filter($"amount" > 2).show()

  // Count people by food
  dataFrame.groupBy("food").count().show()

  // Register the DataFrame as a SQL temporary view
  dataFrame.createOrReplaceTempView("people")

  spark.sql("SELECT * FROM people").show()
}
