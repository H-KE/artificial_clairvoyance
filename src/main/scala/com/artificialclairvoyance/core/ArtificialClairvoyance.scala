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
    // Set up spark context
    val conf = new SparkConf().setAppName("Artificial Clairvoyance")
    val sc = new SparkContext(conf)

    /**
     * Collect data
     * TODO: This should be abstracted to a data collector (specifies where the data comes from)
     * TODO: The filepath should not be hard coded
     */
    /* mlb */
    val battingFile = "src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv"
    val rawBattingData = sc.textFile(battingFile, 2).cache()
    val mlbCentersOutput = "src/test/resources/output/mlb_centers.csv"
    val mlbPlayersHistoricalOutput = "src/test/resources/output/mlb_players_historical.csv"
    val mlbPlayers2014Output = "src/test/resources/output/mlb_players2014.csv"
    /* nba */
    val basketballFile = "src/test/resources/nba/leagues_NBA_2015_per_game_per_game.csv"
    val rawNbaData = sc.textFile(basketballFile, 2).cache()
    val nbaCentersOutput = "src/test/resources/output/nba_centers.csv"
    val nbaPlayersOutput = "src/test/resources/output/nba_players.csv"

    /**
     * Parse the necessary data from the collected data.
     * Get it ready for the machine learning algorithm
     * TODO: Need to abstract the parsing in a different object
     */
    /* mlb */
    // Only get data for 2014 and with games played greater than 100 (to reduce noise)
    // Used for prediction
    val batters2014 = rawBattingData.map(_.split(","))
      .filter(line => line(1).forall(_.isDigit) && line(1).toInt.equals(2014) && line(5).toInt >= 100)
    // Get historical data for mlb. This is what we will use to train our models
    val battersHistorical = rawBattingData.map(_.split(","))
      .filter(line => line(1).forall(_.isDigit) && line(1).toInt >= 1980 && line(1).toInt <= 2014 && line(5).toInt >= 100)
    // parsedData contains the metrics we are interested in, in vector form for each player.
    val trainingBattingData = battersHistorical.map {
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
     * Clustering algorithm. Cluster every seasonal performance by every player.
     * This will return a model that can give the type of seasonal performance,
     * and the grouping of every seasonal performance.
     * TODO: Abstract the machine learning portion
     */
    // Cluster using K-means
    /* mlb */
    val iterationCountMLB = 1000
    val clusterCountMLB = 20
    // Produce the MLB clustering model
    val mlbClusterModel = KMeans.train(trainingBattingData, clusterCountMLB, iterationCountMLB)
    // Find centers of each cluster
    val mlbClusterCenter = mlbClusterModel.clusterCenters map (_.toArray)
    val mlbPlayers =  battersHistorical.map{
      line => Array(
        // Metadata
        Array(
          // ID
          line(0),
          // Season
          line(1),
          // Age
          line(22),
          // Group, Predict using the model
          mlbClusterModel.predict(Vectors.dense(Array(line(8),line(11)).map(_.toDouble)))
        ),
        // Actual data
        Array(
          // Hit
          line(8),
          // HR
          line(11)
        )
      )
    }
    // Group every player
    val mlbGroups = mlbPlayers.groupBy{
      // group by cluster ID
      player => player(0)(3)
    }.collect()

    /* nba */
    val iterationCountNBA = 10000
    val clusterCountNBA = 20
    // Produce the NBA clustering model
    val nbaClusterModel = KMeans.train(parsedNbaData, clusterCountNBA, iterationCountNBA)
    // Find centers of each cluster
    val nbaClusterCenter = nbaClusterModel.clusterCenters map (_.toArray)
    /**
     * Matching algorithm. Given current players and their past seasonal performances,
     * we match the player's most recent season(s) with a cluster from the previous step.
     * Then we find players from that cluster who's had a similar age (e.g. +/- 1yrs?) when they had this season type.
     * Return the current players and their corresponding list of "similar players"
     * TODO: Find the list of players of that cluster with similar age
     * TODO: Abstract this out
     */
    /* mlb */
    val mlbPlayers2014ByGroup = batters2014.map{
      line => Array(
        // Metadata
        Array(
          // ID
          line(0),
          // Season
          line(1),
          // Age
          line(22),
          // Computed Group
          mlbClusterModel.predict(Vectors.dense(Array(line(8),line(11)).map(_.toDouble)))
        ),
        // Actual data
        Array(
          // Hit
          line(8),
          // HR
          line(11)
        )
      )
    }.groupBy{
      // Group by the group for later uses
      player => player(0)(3)
    }.collect()

    /* nba */
    val nbaPlayersByGroup = nba2014.map{
      line => Array(
        // Metadata
        Array(
          line(1)
        ),
        // Actual Data
        Array(
          line(10),
          line(11),
          line(20),
          line(23),
          line(24),
          line(25),
          line(26),
          line(27),
          line(29)
        )
      )
    }.groupBy{
      // Predict using the actual data
      player => nbaClusterModel.predict(Vectors.dense(player(1).map(_.toDouble)))
    }.collect()

    /**
     * Regression Algorithm.
     * Create a predictive model of each player from historical careers of their "similar players"
     * TODO: Implement it
     * TODO: abstract
     */

    /**
     * Output the results
     * TODO: Print for now, but save it to some output file when it's ready
     * TODO: optional... visualize the data (for 2-d data)
     */
    /* mlb */
    // Print total cost
    println("Cost of the MLB Model: %s".format(mlbClusterModel.computeCost(trainingBattingData)))
    
    // Create a Document to represent the data
    // Print the average stat for each group
    printToFile(new File(mlbCentersOutput)) {
      p => {
        p.println("hits,homeruns")
        mlbClusterCenter.foreach(center => p.println("%s,%s".format(center(0), center(1))))
      }
    }
    printToFile(new File(mlbPlayers2014Output)) {
      p => {
        p.println("cluster,player,hits,homeruns,age,similarPlayers")
        for((group, players) <- mlbPlayers2014ByGroup) {
          players.foreach(player => p.println("%s,%s,%s,%s,%s,%s".format(
            group,
            player(0)(0),
            player(1)(0),
            player(1)(1),
            player(0)(2),
            mlbPlayers
              .filter(
                line => line(0)(3).equals(group)
                && line(0)(2).toString.toInt >= player(0)(2).toString.toInt - 1
                && line(0)(2).toString.toInt <= player(0)(2).toString.toInt + 1)
              .map(similarPlayer => similarPlayer(0)(0))
              .reduce((similarPlayerId1, similarPlayerId2) => similarPlayerId1.toString + ";" + similarPlayerId2)
          )))
        }
      }
    }
    printToFile(new File(mlbPlayersHistoricalOutput)) {
      p => {
        p.println("cluster,player,season,hits,homeruns,age")
        for((group, players) <- mlbGroups) {
          players.foreach(player => p.println("%s,%s,%s,%s,%s,%s".format(group, player(0)(0),player(0)(1), player(1)(0), player(1)(1), player(0)(2))))
        }
      }
    }

    /* nba */
    // Print total cost
    println("Cost of the NBA Model: %s".format(nbaClusterModel.computeCost(parsedNbaData)))

    // Create a Document to represent the data
    // Print the average stat for each group
    printToFile(new File(nbaCentersOutput)) {
        p => {
            p.println("fg,3pm,ft,reb,ast,stl,blk,tov,pts")
            nbaClusterCenter.foreach(center => p.println("%s,%s,%s,%s,%s,%s,%s,%s,%s"
                .format(center(0), center(1), center(2), center(3), center(4), center(5), center(6), center(7), center(8))))
        }
    }
    printToFile(new File(nbaPlayersOutput)) {
        p => {
            p.println("cluster,player,fg,3pm,ft,reb,ast,stl,blk,tov,pts")
            for((group, players) <- nbaPlayersByGroup) {
                players.foreach(player => p.println("%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f".format(group, player(0)(0), player(1)(0).toDouble, player(1)(1).toDouble, player(1)(2).toDouble, player(1)(3).toDouble, player(1)(4).toDouble, player(1)(5).toDouble, player(1)(6).toDouble, player(1)(7).toDouble, player(1)(8).toDouble)))
            }
        }
    }

    // Terminate the spark context
    sc.stop()
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}


