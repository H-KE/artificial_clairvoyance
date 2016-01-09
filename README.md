#artificial-clairvoyance
ECE496 Project for predicting future sports statistics using a distributed machine learning stack in Spark.

## Prerequisites
This application is written in Scala and uses sbt to manage the build. You'll need to [install sbt](http://www.scala-sbt.org/).

Or alternatively, on Mac OSX you can install it using [homebrew](http://brew.sh/):
```
brew install sbt
```

You'll also want to get [Apache Spark 1.5.2](http://spark.apache.org/docs/1.5.2/index.html).
Follow the instructions on the [building spark page](http://spark.apache.org/docs/1.5.2/building-spark.html).
You'll need [Java 7+](http://www.java.com/) and [Maven 3.3+](https://maven.apache.org/)

## Build
`sbt package` will build the jar containing the application.

## Run it on Spark
You can use the spark-submit script in the Spark bin directory:
```
${SPARK_HOME}/bin/spark-submit --class "com.artificialclairvoyance.core.ArtificialClairvoyance" --master local[4] target/scala-2.10/artificial-clairvoyance_2.10-0.0.1.jar
```

## Visualization
You need to be able to serve the index.html locally.
Can use http-server:
npm install http-server -g (only need to do this once)
http-server (in artificial clairvoyance directory)
// TODO: Make this more specific/add a script to run the application, add App configurations
