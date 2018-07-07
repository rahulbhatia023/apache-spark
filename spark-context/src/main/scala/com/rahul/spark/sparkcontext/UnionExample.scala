package com.rahul.spark.sparkcontext

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object UnionExample extends App {
  val sparkContext: SparkContext = InitializeSpark.getSparkContext("employee", "local")

  val employees1: RDD[String] = sparkContext.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\EMP1.csv")
  /*
  Array[String] = Array(
  100,Steven,King,SKING,515.123.4567,17-JUN-87,AD_PRES,24000,,90,
  101,Neena,Kochhar,NKOCHHAR,515.123.4568,21-SEP-89,AD_VP,17000,100,90,
  102,Lex,De Haan,LDEHAAN,515.123.4569,13-JAN-93,AD_VP,17000,100,90,
  103,Alexander,Hunold,AHUNOLD,590.423.4567,03-JAN-90,IT_PROG,9000,102,60,
  104,Bruce,Ernst,BERNST,590.423.4568,21-MAY-91,IT_PROG,6000,103,60,
  105,David,Austin,DAUSTIN,590.423.4569,25-JUN-97,IT_PROG,4800,103,60,
  ...)
  */

  val employees2: RDD[String] = sparkContext.textFile("D:\\Softwares\\hadoop-2.8.4\\data\\EMP2.csv")
  /*
  Array[String] = Array(
  181,Jean,Fleaur,JFLEAUR,650.507.9877,23-FEB-98,SH_CLERK,3100,120,50,
  182,Martha,Sullivan,MSULLIVA,650.507.9878,21-JUN-99,SH_CLERK,2500,120,50,
  183,Girard,Geoni,GGEONI,650.507.9879,03-FEB-00,SH_CLERK,2800,120,50,
  184,Nandita,Sarchand,NSARCHAN,650.509.1876,27-JAN-96,SH_CLERK,4200,121,50,
  185,Alexis,Bull,ABULL,650.509.2876,20-FEB-97,SH_CLERK,4100,121,50,
  ...)
  */

  val union: RDD[String] = employees1.union(employees2)
  /*
  Array[String] = Array(
  100,Steven,King,SKING,515.123.4567,17-JUN-87,AD_PRES,24000,,90,
  101,Neena,Kochhar,NKOCHHAR,515.123.4568,21-SEP-89,AD_VP,17000,100,90,
  102,Lex,De Haan,LDEHAAN,515.123.4569,13-JAN-93,AD_VP,17000,100,90,
  .....
  .....
  181,Jean,Fleaur,JFLEAUR,650.507.9877,23-FEB-98,SH_CLERK,3100,120,50,
  182,Martha,Sullivan,MSULLIVA,650.507.9878,21-JUN-99,SH_CLERK,2500,120,50,
  183,Girard,Geoni,GGEONI,650.507.9879,03-FEB-00,SH_CLERK,2800,120,50,
  .....)
  */
}
