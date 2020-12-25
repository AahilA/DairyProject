import org.apache.spark.sql.SparkSession

object Main{
  def main(args: Array[String]) {
    println(System.getProperty("user.dir"));
    val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    val textFile = spark.read.textFile("test.txt")
    println("TEXT FILE LENGTH: ")
    println(textFile.count())
    spark.close()
  }
}