package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object JoinExample extends App {
  val sparkContext: SparkContext = InitializeSpark.getSparkContext("employee", "local")

  val employees: RDD[String] = sparkContext.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/EMP1.csv")
  /*
  Array[String] = Array(
  100,Steven,King,SKING,515.123.4567,17-JUN-87,AD_PRES,24000,,90,
  101,Neena,Kochhar,NKOCHHAR,515.123.4568,21-SEP-89,AD_VP,17000,100,90,
  102,Lex,De Haan,LDEHAAN,515.123.4569,13-JAN-93,AD_VP,17000,100,90,
  103,Alexander,Hunold,AHUNOLD,590.423.4567,03-JAN-90,IT_PROG,9000,102,60,
  104,Bruce,Ernst,BERNST,590.423.4568,21-MAY-91,IT_PROG,6000,103,60,
  105,David,Austin,DAUSTIN,590.423.4569,25-JUN-97,IT_PROG,4800,103,60,
  .......)
  */

  val departments: RDD[String] = sparkContext.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/DEPT.csv")
  /*
  Array[String] = Array(
  50,JAVA_DEPARTMENT)
   */

  val employees_departmentID: RDD[(String, Array[String])] = employees.map(line => line.split(",")).map(line => (line(9), line))
  /*
  Array[(String, Array[String])] = Array(
  (90,Array(100, Steven, King, SKING, 515.123.4567, 17-JUN-87, AD_PRES, 24000, "", 90)),
  (90,Array(101, Neena, Kochhar, NKOCHHAR, 515.123.4568, 21-SEP-89, AD_VP, 17000, 100, 90)),
  (90,Array(102, Lex, De Haan, LDEHAAN, 515.123.4569, 13-JAN-93, AD_VP, 17000, 100, 90)),
  (60,Array(103, Alexander, Hunold, AHUNOLD, 590.423.4567, 03-JAN-90, IT_PROG, 9000, 102, 60)),
  (60,Array(104, Bruce, Ernst, BERNST, 590.423.4568, 21-MAY-91, IT_PROG, 6000, 103, 60)),
  ...)
  */

  val departments_departmentID: RDD[(String, Array[String])] = departments.map(line => line.split(",")).map(line => (line(0), line))
  /*
  Array[(String, Array[String])] = Array(
  (50,Array(50, JAVA_DEPARTMENT)))
   */

  val employees_departments_join = departments_departmentID.join(employees_departmentID)
  /*
  Array[(String, (Iterable[Array[String]], Iterable[Array[String]]))] = Array(
  (60,(CompactBuffer(),CompactBuffer([Ljava.lang.String;@49f82f95, [Ljava.lang.String;@73c72238, [Ljava.lang.String;@3e6be3c1, [Ljava.lang.String;@112188cc, [Ljava.lang.String;@19ad99fa))),
  (80,(CompactBuffer(),CompactBuffer([Ljava.lang.String;@5331be15, [Ljava.lang.String;@32a53a59, [Ljava.lang.String;@2f5fb329, [Ljava.lang.String;@5ac88d71, [Ljava.lang.String;@457512b, [Ljava.lang.String;@1f3bd40a, [Ljava.lang.String;@4dafea3f, [Ljava.lang.String;@531245fe, [Ljava.lang.String;@6d7298be, [Ljava.lang.String;@2c289a9e, [Ljava.lang.String;@1ee47336, [Ljava.lang.String;@5f18f8a1, [Ljava.lang.String;@26d63c94, [Ljava.lang.String;@4e42beba, [Ljava.lang.String;@73021987, [Ljava.lang.String;@6bc72ab6, [Ljava.lang.S...
   */
}
