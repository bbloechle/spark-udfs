name := "spark-udfs"

version := "0.1.0"

organization := "com.cloudera.education"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers += "MavenRepository" at "http://mvnrepository.com"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)
