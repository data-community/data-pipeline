package datacommunity.datapipeline

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
    spark.read
      .json(inputPath)
      .write
      .mode(SaveMode.Overwrite)
      .csv(outputPath)
  }
}
