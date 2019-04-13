package datacommunity.datapipeline

import java.nio.file.Path

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{AndFileFilter, EmptyFileFilter, SuffixFileFilter, TrueFileFilter}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

import java.nio.file.{Files, Path, StandardOpenOption}

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{AndFileFilter, EmptyFileFilter, SuffixFileFilter, TrueFileFilter}


class DefaultFeatureSpecWithSpark extends FeatureSpec with GivenWhenThen with Matchers {
  val spark: SparkSession = SparkSession.builder
    .appName("Spark Test App")
    .config("spark.driver.host","127.0.0.1")
    .master("local")
    .getOrCreate()

  def readCSV(path: Path): Set[String] = {
    import scala.collection.JavaConverters._

    val files = FileUtils
      .listFiles(path.toFile,
        new AndFileFilter(EmptyFileFilter.NOT_EMPTY,
          new SuffixFileFilter(".csv")),
        TrueFileFilter.TRUE)
      .asScala

    val allLines = files
      .foldRight(Set[String]())((file, lineSet) =>
        lineSet ++ FileUtils.readLines(file).asScala)
      .map(_.trim)

    allLines
  }

  def writeLines(lines: Seq[String], filePath: Path): Path = {
    import scala.collection.JavaConverters._
    Files.write(filePath, lines.asJava, StandardOpenOption.CREATE)
  }
}
