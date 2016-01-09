name := "Artificial Clairvoyance"

version := "0.0.1"

scalaVersion := "2.10.6"

organization := "com.artificialclairvoyance.core"

resolvers += "releases"  at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.2",
    "org.apache.spark" %% "spark-mllib" % "1.5.2",
    "org.mongodb" %% "casbah" % "2.8.2"
)