import sbt.Keys._
//import sun.security.util.PathList

name := "twitterStreaming"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.11" % "2.1.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.1",
  "org.twitter4j" % "twitter4j-stream" % "3.0.3",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.json4s" %% "json4s-native" % "3.2.11",
  "joda-time" % "joda-time" % "2.7",
  "org.joda" % "joda-convert" % "1.2"
)

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Repository" at "http://repo.akka.io/releases/"
)

mainClass in (Compile, run) := Some("com.finalProject.group1.twitterStreaming")