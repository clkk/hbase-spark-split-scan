name := "hbase-spark-split-scan"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Cloudera Repos" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

test in assembly := {}

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase" % "1.2.0-cdh5.13.2",
  "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.13.2",
  "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.13.2",
  "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.13.2",
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0"
)

mainClass in assembly := Some("stackov")

// META-INF discarding
assemblyMergeStrategy in assembly := {

  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first

}