name := "SparkMonListener"
version := "1.0"
scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"
val kafkaVersion = "0.8.2.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % sparkVersion,
  "org.apache.kafka" % "kafka_2.10" % kafkaVersion,

  "org.slf4j" % "slf4j-api" % "1.7.21"

)

