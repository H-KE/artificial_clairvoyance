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
Or if you have your SPARK_HOME set, you can run it using the run.sh script under /bin
```
export SPARK_HOME='<SPARK LOCATION>'
./bin/run.sh
```

## Application

There is a front-end under 'app' directory

To see host a local version of the app:
```
npm install (only do this once)
bower isntall (only do this once)
grunt serve (this requires ruby and compass to be isntalled)
```
