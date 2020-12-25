
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {


  def main(args: Array[String]) {
    println("Hello, world!") // prints Hello World
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()



  }
}
