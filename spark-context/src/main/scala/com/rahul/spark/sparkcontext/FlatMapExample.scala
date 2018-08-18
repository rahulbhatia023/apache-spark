package com.rahul.spark.sparkcontext

object FlatMapExample extends App {
  val sc = InitializeSpark.getSparkContext("employee", "local")

  val employeeRDD = sc.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\employee.txt")
  /*
  Array[String] = Array(
  001,Rajiv,Reddy,21,programmer,003,
  002,siddarth,Battacharya,22,programmer,003,
  003,Rajesh,Khanna,22,programmer,003,
  004,Preethi,Agarwal,21,programmer,003,
  005,Trupthi,Mohanthy,23,programmer,003,
  006,Archana,Mishra,23,programmer,003,
  007,Komal,Nayak,24,teamlead,002,
  008,Bharathi,Nambiayar,24,manager,001)
   */

  val employeeMapRDD = employeeRDD.flatMap(line => line.split(","))
  /*
  Array[String] = Array(
  001,
  Rajiv,
  Reddy,
  21,
  programmer,
  003,
  002,
  siddarth,
  Battacharya,
  ....
  */
}