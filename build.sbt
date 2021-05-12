name := "sentiment-analysis-nlpcore"

version := "0.1"

scalaVersion := "2.12.10"

useCoursier := false

val sparkVersion = "3.0.0"
val twitter4jVersion = "4.0.7"
val log4jVersion = "2.4.1"
val nlpLibVersion = "3.5.1"

resolvers ++= Seq(
	"bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
	"Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
	"MavenRepository" at "https://mvnrepository.com"
)

/*
  Beware that if you're working on this repository from a work computer,
  corporate firewalls might block the IDE from downloading the libraries and/or the Docker images in this project.
 */
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,

	// streaming
	"org.apache.spark" %% "spark-streaming" % sparkVersion,

	// streaming-kafka
	"org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

	// low-level integrations
	"org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

	// twitter
	"org.twitter4j" % "twitter4j-core" % twitter4jVersion,
	"org.twitter4j" % "twitter4j-stream" % twitter4jVersion,

	// logging
	"org.apache.logging.log4j" % "log4j-api" % log4jVersion,
	"org.apache.logging.log4j" % "log4j-core" % log4jVersion,

	"edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion,
	"edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion classifier "models"
)