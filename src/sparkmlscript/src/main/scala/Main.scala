import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.DoubleType
import com.mongodb.spark._
import com.mongodb.spark.sql._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import PercentileApprox._

object Main {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder.appName("DA_9").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://jamalhashim:Tp123456789!@cluster0-lavns.azure.mongodb.net/test?retryWrites=true&w=majority", "database" -> "farmer1", "collection" -> "cow10"))
    var mongoDf = spark.read.option("inferschema",true).mongo(readConfig)
    mongoDf.show()

    mongoDf.printSchema()

    mongoDf = mongoDf.sort($"timestamp".desc)

    mongoDf = mongoDf.withColumn("Date",to_date(from_unixtime($"timestamp")))
            .select("Date", mongoDf.columns:_*)
            .drop("timestamp")

    mongoDf = mongoDf.select($"Date", explode($"cow").alias("Cow"), $"milk")
                      .select($"Date", $"Cow", explode($"milk").alias("Milk"))
                      .select($"Date",   
                       $"Cow.low_pass_over_activity", 
                       $"Cow.temp_without_drink_cycles",
                       $"Cow.animal_activity",
                       $"Milk.Yield(gr)",
                       $"Milk.Fat(%)")

    mongoDf.show()

    mongoDf.groupBy("Date")
    .agg(
      percentile_approx($"low_pass_over_activity", lit(0.5)).alias("L"),
      percentile_approx($"temp_without_drink_cycles", lit(0.5)).alias("TW"),
      percentile_approx($"animal_activity", lit(0.5)).alias("A"),
      percentile_approx($"Yield(gr)", lit(0.5)).alias("Y"),
      percentile_approx($"Fat(%)", lit(0.5)).alias("F"),
      stddev($"low_pass_over_activity").alias("SL"),
      stddev($"temp_without_drink_cycles").alias("STW"),
      stddev($"animal_activity").alias("SA"),
      stddev($"Yield(gr)").alias("SY"),
      stddev($"Fat(%)").alias("SF"),
    ).filter((col("L") <= lit(3) * col("SL") + col("L") && col("L") >= col("L") - lit(3) * col("SL"))||
             (col("TW") <= lit(3) * col("STW") + col("TW") && col("TW") >= col("TW") - lit(3) * col("STW")) ||
             (col("A") <= lit(3) * col("SA") + col("A") && col("A") >= col("A") - lit(3) * col("SA")) ||
             (col("Y") <= lit(3) * col("SY") + col("Y") && col("Y") >= col("Y") - lit(3) * col("SY")) ||
             (col("F") <= lit(3) * col("SF") + col("F") && col("F") >= col("F") - lit(3) * col("SF"))).show()

    mongoDf = mongoDf.groupBy("Date")
              .agg(
                avg($"low_pass_over_activity").alias("low_pass_over_activity"),
                avg($"temp_without_drink_cycles").alias("temp_without_drink_cycles"),
                stddev($"temp_without_drink_cycles").alias("temp_without_drink_cycles_std"),
                avg($"animal_activity").alias("animal_activity"),
                avg($"Yield(gr)").alias("Yield(gr)"),
                avg($"Fat(%)").alias("Fat(%)")
                )
    
    mongoDf = mongoDf.limit(7)

    mongoDf.show()

    var df = spark.sqlContext.read
      .format("csv")
      .option("header",true)
      .option("inferschema",true)
      .load("../../preprocData/aggregatedData/cowID10.csv")
    df = df.filter(df.col("Yield(gr)").notEqual(0))
    df = df.withColumn("temp_without_drink_cycles",df.col("temp_without_drink_cycles").cast(DoubleType))
    df = df.withColumn("temp_without_drink_cycles_std",df.col("temp_without_drink_cycles_std").cast(DoubleType))
    df = df.withColumn("animal_activity",df.col("animal_activity").cast(DoubleType))
    df = df.withColumn("low_pass_over_activity",df.col("low_pass_over_activity").cast(DoubleType))
    df = df.withColumn("Yield(gr)",df.col("Yield(gr)").cast(DoubleType))
    df = df.withColumn("Fat(%)",df.col("Fat(%)").cast(DoubleType))
  //  df.show(10)

    val assembler1 = new VectorAssembler().
      setInputCols(Array( "temp_without_drink_cycles","temp_without_drink_cycles_std", "animal_activity", "low_pass_over_activity")).
      setOutputCol("features").
      transform(df)


    mongoDf = mongoDf.withColumn("temp_without_drink_cycles",mongoDf.col("temp_without_drink_cycles").cast(DoubleType))
    mongoDf = mongoDf.withColumn("temp_without_drink_cycles_std",mongoDf.col("temp_without_drink_cycles_std").cast(DoubleType))
    mongoDf = mongoDf.withColumn("animal_activity",mongoDf.col("animal_activity").cast(DoubleType))
    mongoDf = mongoDf.withColumn("low_pass_over_activity",mongoDf.col("low_pass_over_activity").cast(DoubleType))
    mongoDf = mongoDf.withColumn("Yield(gr)",mongoDf.col("Yield(gr)").cast(DoubleType))
    mongoDf = mongoDf.withColumn("Fat(%)",mongoDf.col("Fat(%)").cast(DoubleType))


    val assembler2 = new VectorAssembler().
      setInputCols(Array( "temp_without_drink_cycles","temp_without_drink_cycles_std", "animal_activity", "low_pass_over_activity")).
      setOutputCol("features").
      transform(mongoDf)

    assembler2.show()

    val lr = new LinearRegression().setLabelCol("Fat(%)").setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val model = lr.fit(assembler1)
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //trainingSummary.residuals.show(1000)
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    model.evaluate(assembler2).predictions.show()
    spark.sparkSession.stop()
    spark.stop()
  }

}

object PercentileApprox {
  def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column = {
    val expr = new ApproximatePercentile(
      col.expr,  percentage.expr, accuracy.expr
    ).toAggregateExpression
    new Column(expr)
  }
  def percentile_approx(col: Column, percentage: Column): Column = percentile_approx(
    col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  )
}