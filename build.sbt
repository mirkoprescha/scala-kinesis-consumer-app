name := "rt-metric-aggregator"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.amazonaws" % "amazon-kinesis-client" % "1.7.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.4.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21"
