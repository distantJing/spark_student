name := "test"

version := "1.0"

scalaVersion := "2.11.9"
val sparkVersion = "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion