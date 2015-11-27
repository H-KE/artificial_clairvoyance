package com.artificialclairvoyance.core

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

/**
 * Top level application container.
 * This will set up the necessary context and invoke our application
 * TODO: this is just a stub right now
 **/
object ArtificialClairvoyance {
  def main(args: Array[String]) {
    // Set up spark context
    val conf = new SparkConf().setAppName("Artificial Clairvoyance")
    val sc = new SparkContext(conf)

    // Should be some file on your system
    // TODO: This should be abstracted to a data collector (specifies where the data comes from)
    val battingFile = "src/test/resources/lahman-csv_2015-01-24/Batting.csv"
    val rawBattingData = sc.textFile(battingFile, 2).cache()

    // Parse the necessary data from this
    // TODO: Need to abstract the parsing in a different object
    val batters2014 = rawBattingData.map(_.split(",")).filter(line => line(1).equals("2014")&&line(5).toInt>=100)
    val parsedData = batters2014.map {
      line =>
        Vectors.dense(line(12).toDouble, line(16).toDouble)
    }

    // Run it through K-MEANS
    // TODO: Abstract the machine learning portion
    val iterationCount = 1000
    val clusterCount = 10
    val clusterModel = KMeans.train(parsedData, clusterCount, iterationCount)
    val clusterCenter = clusterModel.clusterCenters map (_.toArray)
    println("Cost of this Model: %s".format(clusterModel.computeCost(parsedData)))
    clusterCenter.foreach(center => println("Cluster Center: (RBI: %s, SO: %s)".format(center(0), center(1))))

    // TODO: Visualize data
  }
}
