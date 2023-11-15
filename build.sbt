
ThisBuild / organization := "com.bdf.custom"
ThisBuild / version := "0.0.2"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / resolvers += Resolver.mavenLocal

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.4.7",
    "org.apache.spark" % "spark-sql_2.11" % "2.4.7",
    "org.apache.hadoop" % "hadoop-client" % "2.2.0",
    "org.apache.hadoop" % "hadoop-common" % "3.1.4"
);

