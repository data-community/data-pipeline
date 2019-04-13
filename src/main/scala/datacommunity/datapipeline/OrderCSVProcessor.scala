package datacommunity.datapipeline

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object OrderCSVProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Order CSV Processor")
      .config("spark.driver.host", "127.0.0.1")
      .master("local")
      .getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)

    run(spark, inputPath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val schema = StructType(Seq(
      StructField("OrderId", StringType),
      StructField("Title", StringType),
      StructField("Quantity", IntegerType),
      StructField("CreateTime", TimestampType)
    ))

    import spark.implicits._

    spark.read
      .schema(schema).option("header", "true")
      .csv(inputPath)
      .withColumn("Date", $"CreateTime".cast("date"))
      .select($"Title", $"Quantity", $"Date")
      .groupBy($"Title", $"Date")
      .sum("quantity")
      .withColumnRenamed("sum(quantity)", "Total")
      .select("Date", "Title", "Total")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath)
  }
}
