name := "data-pipeline"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)

//libraryDependencies ++= sparkDependencies.map(_ % "provided")

libraryDependencies ++= sparkDependencies

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.codehaus.jettison" % "jettison" % "1.4.0",
  "com.typesafe" % "config" % "1.3.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

test in assembly := {}

// https://stackoverflow.com/questions/42310821/deduplicate-different-file-contents-found-error-for-sbt-and-scala
// https://stackoverflow.com/questions/14791955/assembly-merge-strategy-issues-using-sbt-assembly
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}