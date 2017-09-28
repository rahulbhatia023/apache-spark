import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object InitializeSpark extends App {
  val sparkConf = new SparkConf().setAppName("employees").setMaster("local")
  val sparkContext: SparkContext = new SparkContext(sparkConf)

  val employees = sparkContext.textFile("/home/rahul/Softwares/hadoop-2.8.0/data/employee.txt")
  employees.collect().foreach(line => println(line))
}