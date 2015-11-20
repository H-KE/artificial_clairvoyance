name := "Artificial Clairvoyance"

version := "0.0.1"

scalaVersion := "2.10.4"

organization := "com.artificialclairvoyance.core"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.2",
    "org.apache.spark" %% "spark-mllib" % "1.5.2"
)
