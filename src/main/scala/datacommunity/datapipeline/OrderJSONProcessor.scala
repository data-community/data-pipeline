package datacommunity.datapipeline

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object OrderJSONProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Order JSON Processor")
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
      StructField("timestamp", TimestampType),
      StructField("payload", StructType(Seq(
        StructField("orderId", StringType),
        StructField("items", ArrayType(StructType(Seq(
          StructField("title", StringType),
          StructField("quantity", IntegerType)
        ))))
      ))
      )
    ))

    import spark.implicits._

    spark.read
      .schema(schema)
      .json(inputPath)
      .withColumn("orderId", $"payload.orderId")
      .withColumn("item", explode($"payload.items"))
      .withColumn("Title", $"item.title")
      .withColumn("Quantity", $"item.quantity")
      .withColumn("Date", $"timestamp".cast("date"))
      .select($"Title", $"Quantity", $"Date")
      .groupBy($"Title", $"Date")
      .sum("Quantity")
      .withColumnRenamed("sum(Quantity)", "Total")
      .select("Date", "Title", "Total")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath)

  }
}
