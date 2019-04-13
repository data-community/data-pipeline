package datacommunity.datapipeline

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils

class OrderJSONProcessorTest extends DefaultFeatureSpecWithSpark {
  feature("Batch Processing Orders of JSON format") {
    scenario("Calculate the count of each item") {
      Given("A JSON file with only one order of one item, and a known output file")
      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val inputFile = Files.createFile(rootDirectory.resolve("orders.log"))
      val outputDirectory: Path = rootDirectory.resolve("output")

      val lines = List(
        """{"timestamp":"2019-03-30T01:59:40.530Z","type":"orderCreated","payload":{"orderId":"98770e2d8119c308","items":[{"title":"Building a Scalable Data Warehouse with Data Vault 2.0","quantity":1}]}}"""
      )

      writeLines(lines, inputFile)

      When("Trigger the application")

      OrderJSONProcessor.run(spark, inputFile.toUri.toString, outputDirectory.toUri.toString)

      Then("It outputs files containing the expected data")

      val allLines = readCSV(outputDirectory)

      val expectedLines = Set(
        "Date,Title,Total",
        "2019-03-30,Building a Scalable Data Warehouse with Data Vault 2.0,1")

      allLines should contain theSameElementsAs expectedLines
      FileUtils.deleteDirectory(rootDirectory.toFile)
    }
  }
}
