package com.artificialclairvoyance.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import java.io._

/**
 * Top level application container.
 * This will set up the necessary context and invoke our application
 * TODO: this is just a stub right now
 **/
object ArtificialClairvoyance {
  def main(args: Array[String]) {
    // Should be some file on your system
    val logFile = "/Users/kehan/spark/artificial-clairvoyance/README.md"
    // Set up spark context
    val conf = new SparkConf().setAppName("Artificial Clairvoyance")
    val sc = new SparkContext(conf)

    /**
     * Collect data
     * TODO: This should be abstracted to a data collector (specifies where the data comes from)
     * TODO: The filepath should not be hard coded
     */
    /* mlb */
    val battingFile = "src/test/resources/lahman-csv_2015-01-24/Batting.csv"
    val rawBattingData = sc.textFile(battingFile, 2).cache()
    val centersOutput = "src/test/resources/output/centers.csv"
    val playersOutput = "src/test/resources/output/players.csv"
    /* nba */
    val basketballFile = "src/test/resources/nba/leagues_NBA_2015_per_game_per_game.csv"
    val rawNbaData = sc.textFile(basketballFile, 2).cache()

    /**
     * Parse the necessary data from the collected data.
     * Get it ready for the machine learning algorithm
     * TODO: Need to abstract the parsing in a different object
     */
    /* mlb */
    // Only get data for 2014 and with games played greater than 100 (to reduce noise)
    val batters2014 = rawBattingData.map(_.split(","))
      .filter(line => line(1).equals("2014") && line(5).toInt >= 100)
    // parsedData contains the metrics we are interested in, in vector form for each player.
    val parsedBattingData = batters2014.map {
      line => Vectors.dense(
        line(8).toDouble,
        line(11).toDouble
      )
    }
    /* nba */
    val nba2014 = rawNbaData.map(_.split(","))
      .filter(line => !line(0).equals("Rk") && line(7).toDouble >= 20)
    // use FG%(10), 3PM(11), FT%(20), REB(23), AST(24), STL(25), BLK(26), TOV(27), PTS(29)
    val parsedNbaData = nba2014.map {
      line => Vectors.dense(
        line(10).toDouble,
        line(11).toDouble,
        line(20).toDouble,
        line(23).toDouble,
        line(24).toDouble,
        line(25).toDouble,
        line(26).toDouble,
        line(27).toDouble,
        line(29).toDouble
      )
    }

    /**
     * Machine learning algorithms
     * TODO: Abstract the machine learning portion
     */
    // Cluster using K-means
    /* mlb */
    val iterationCountMLB = 1000
    val clusterCountMLB = 20
    // Produce the MLB clustering model
    val mlbClusterModel = KMeans.train(parsedBattingData, clusterCountMLB, iterationCountMLB)
    // Find centers of each cluster
    val mlbClusterCenter = mlbClusterModel.clusterCenters map (_.toArray)
    // Group the actual players into clusters
    val mlbPlayersByGroup = batters2014.map{
      line => Array(
        // Metadata
        Array(
          line(0),
          line(1)
        ),
        // Actual data
        Array(
          line(8),
          line(11)
        )
      )
    }.groupBy{
      player => mlbClusterModel.predict(Vectors.dense(player(1).map(_.toDouble)))
    }.collect()

    /**
     * Output the results
     * TODO: Print for now, but save it to some output file when it's ready
     * TODO: optional... visualize the data (for 2-d data)
     */
    /* mlb */
    // Print total cost
    println("Cost of the MLB Model: %s".format(mlbClusterModel.computeCost(parsedBattingData)))
    // Create a Document to represent the data
    // Print the average stat for each group

    printToFile(new File(centersOutput)) {
        p => {
            p.println("hits,homeruns")
            mlbClusterCenter.foreach(center => p.println("%s,%s".format(center(0), center(1))))
        }
    }

    printToFile(new File(playersOutput)) {
        p => {
            p.println("cluster,player,hits,homeruns")
            for((group, players) <- mlbPlayersByGroup) {
                players.foreach(player => p.println("%s,%s,%s,%s".format(group, player(0)(0), player(1)(0), player(1)(1))))
            }
        }
    }
    // Output each individual player in each group
    // for((group, players) <- mlbPlayersByGroup) {
    //     printToFile(new File(playersOutput)) {
    //         p => players.foreach(player => p.println("%s,%s,%s,%s".format(group, player(0)(0), player(1)(0), player(1)(1))))
    //     }
    // }















    // /* nba */
    // val iterationCountNBA = 10000
    // val clusterCountNBA = 20
    // // Produce the NBA clustering model
    // val nbaClusterModel = KMeans.train(parsedNbaData, clusterCountNBA, iterationCountNBA)
    // // Find centers of each cluster
    // val nbaClusterCenter = nbaClusterModel.clusterCenters map (_.toArray)
    // // Group the actual players into clusters
    // val nbaPlayersByGroup = nba2014.map{
    //   line => Array(
    //     // Metadata
    //     Array(
    //       line(1)
    //     ),
    //     // Actual Data
    //     Array(
    //       line(10),
    //       line(11),
    //       line(20),
    //       line(23),
    //       line(24),
    //       line(25),
    //       line(26),
    //       line(27),
    //       line(29)
    //     )
    //   )
    // }.groupBy{
    //   player => nbaClusterModel.predict(Vectors.dense(player(1).map(_.toDouble)))
    // }.collect()

    // /* nba */
    // // Print total cost
    // println("Cost of the NBA Model: %s".format(nbaClusterModel.computeCost(parsedNbaData)))
    // // Print the average stat for each group
    // nbaClusterCenter.foreach(
    //   center => println(
    //     "Cluster Center: (FG: %.2f, 3PM: %.2f, FT: %.2f, REB: %.2f, AST: %.2f, STL: %.2f, BLK: %.2f, TOV: %.2f, PTS: %.2f)"
    //       .format(center(0), center(1), center(2), center(3), center(4), center(5), center(6), center(7), center(8))
    //   )
    // )
    // // Output each individual player in each group
    // for((group, players) <- nbaPlayersByGroup) {
    //   players.foreach(
    //     player =>
    //       println(
    //       "Group %s - Name: %s (FG: %.2f, 3PM: %.2f, FT: %.2f, REB: %.2f, AST: %.2f, STL: %.2f, BLK: %.2f, TOV: %.2f, PTS: %.2f)"
    //         .format(group, player(0)(0), player(1)(0).toDouble, player(1)(1).toDouble, player(1)(2).toDouble, player(1)(3).toDouble, player(1)(4).toDouble, player(1)(5).toDouble, player(1)(6).toDouble, player(1)(7).toDouble, player(1)(8).toDouble)
    //     )
    //   )
    // }

    // Terminate the spark context
    sc.stop()
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}


