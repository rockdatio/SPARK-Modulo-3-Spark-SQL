name := "Example"

version := "1.0"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)