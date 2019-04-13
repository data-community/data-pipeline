package datacommunity.datapipeline

import java.nio.file.{Files, StandardOpenOption}

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{AndFileFilter, EmptyFileFilter, SuffixFileFilter, TrueFileFilter}

class OrderCSVProcessorTest extends DefaultFeatureSpecWithSpark {
  feature("Batch Processing Orders") {
    scenario("Calculate the count of each item") {
      Given("A CSV file with only one order of one item, and a known output file")
      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val inputFile = Files.createFile(rootDirectory.resolve("orders.csv"))
      val outputDirectory = rootDirectory.resolve("output")

      import scala.collection.JavaConverters._
      val lines = List(
        "OrderId,Title,Quantity,CreateTime",
        "98770e2d8119c308,\"Building a Scalable Data Warehouse with Data Vault 2.0\",1,2019-03-30T01:59:40.530Z"
      )
      Files.write(inputFile, lines.asJava, StandardOpenOption.CREATE)

      When("Trigger the application")

      OrderCSVProcessor.run(spark, inputFile.toUri.toString, outputDirectory.toUri.toString)

      Then("It outputs files containing the expected data")

      val files = FileUtils
        .listFiles(outputDirectory.toFile,
          new AndFileFilter(EmptyFileFilter.NOT_EMPTY,
            new SuffixFileFilter(".csv")),
          TrueFileFilter.TRUE)
        .asScala

      val allLines = files
        .foldRight(Set[String]())((file, lineSet) =>
          lineSet ++ FileUtils.readLines(file).asScala)
        .map(_.trim)

      val expectedLines = Set(
        "Date,Title,Total",
        "2019-03-30,Building a Scalable Data Warehouse with Data Vault 2.0,1")

      allLines should contain theSameElementsAs expectedLines
      FileUtils.deleteDirectory(rootDirectory.toFile)
    }
  }
}
