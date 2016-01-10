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
    // Inputs
    val battingFile = "src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv"
    val rawBattingData = sc.textFile(battingFile, 2).cache()
    // Location of output files... TODO: Remove these when database is running
    val mlbCentersOutput = "src/test/resources/output/mlb_centers.csv"
    val mlbPlayersHistoricalOutput = "src/test/resources/output/mlb_players_historical.csv"
    val mlbPlayers2014Output = "src/test/resources/output/mlb_players2014.csv"

    /* nba */
    // Inputs
    val nbaFile = "src/test/resources/nba/nbaPlayerTotals.csv"
    val rawNbaData = sc.textFile(nbaFile, 2).cache()
    // Location of output files... TODO: Remove these when database is running
    val nbaCentersOutput = "src/test/resources/output/nba_centers.csv"
    val nbaPlayersHistoricalOutput = "src/test/resources/output/nba_players_historical.csv"
    val nbaPlayers2014Output = "src/test/resources/output/nba_players2014.csv"

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
    // Only get data for 2014 and with games played greater than 20
    // Used for prediction
    val nba2014 = rawNbaData.map(_.split(","))
      .filter(line => line(2).forall(_.isDigit) && line(2).toInt.equals(2014) && line(2).toDouble >= 20)
      .map(line => Array(
        line(0),// (0)PlayerId,
        line(1),// (1)Name,
        line(2),// (2)Season,
        line(3),// (3)Age,
        line(4),// (4)Position,
        line(5),// (5)Team,
        line(6),// (6)Games,
        line(7),// (7)MP,
        (line(8).toDouble*100).toString,// (8)FG%,
        (line(9).toDouble/line(6).toDouble).toString,// (9)3PM per game,
        (line(10).toDouble*100).toString,// (10)3P%,
        (line(11).toDouble*100).toString,// (11)FT%,
        (line(12).toDouble/line(6).toDouble).toString,// (12)REB per game,
        (line(13).toDouble/line(6).toDouble).toString,// (13)AST per game,
        (line(14).toDouble/line(6).toDouble).toString,// (14)STL per game,
        (line(15).toDouble/line(6).toDouble).toString,// (15)BLK per game,
        (line(16).toDouble/line(6).toDouble).toString,// (16)TOV per game,
        (line(17).toDouble/line(6).toDouble).toString// (17)PTS per game
      ))
    // Get historical data for nba. This is what we will use to train our models
    val nbaHistorical = rawNbaData.map(_.split(","))
      .filter(line => line(2).forall(_.isDigit) && line(2).toDouble >= 20)
      .map(line => Array(
        line(0),// (0)PlayerId,
        line(1),// (1)Name,
        line(2),// (2)Season,
        line(3),// (3)Age,
        line(4),// (4)Position,
        line(5),// (5)Team,
        line(6),// (6)Games,
        line(7),// (7)MP,
        (line(8).toDouble*100).toString,// (8)FG%,
        (line(9).toDouble/line(6).toDouble).toString,// (9)3PM per game,
        (line(10).toDouble*100).toString,// (10)3P%,
        (line(11).toDouble*100).toString,// (11)FT%,
        (line(12).toDouble/line(6).toDouble).toString,// (12)REB per game,
        (line(13).toDouble/line(6).toDouble).toString,// (13)AST per game,
        (line(14).toDouble/line(6).toDouble).toString,// (14)STL per game,
        (line(15).toDouble/line(6).toDouble).toString,// (15)BLK per game,
        (line(16).toDouble/line(6).toDouble).toString,// (16)TOV per game,
        (line(17).toDouble/line(6).toDouble).toString// (17)PTS per game
      ))
    // use FG%(8), 3PM(9), FT%(11), REB(12), AST(13), STL(14), BLK(15), TOV(16), PTS(17)
    val trainingNbaData = nbaHistorical.map {
      line => Vectors.dense(
        line(8).toDouble,
        line(9).toDouble,
        line(11).toDouble,
        line(12).toDouble,
        line(13).toDouble,
        line(14).toDouble,
        line(15).toDouble,
        line(16).toDouble,
        line(17).toDouble
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
    val mlbPlayers = battersHistorical.map{
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
    val nbaClusterModel = KMeans.train(trainingNbaData, clusterCountNBA, iterationCountNBA)
    // Find centers of each cluster
    val nbaClusterCenter = nbaClusterModel.clusterCenters map (_.toArray)
    val nbaPlayers = nbaHistorical.map{
      line => Array(
        // Metadata
        Array(
          // ID
          line(0),
          // Name
          line(1),
          // Season
          line(2),
          // Age
          line(3),
          // Computed Group
          nbaClusterModel.predict(Vectors.dense(Array(line(8), line(9), line(11), line(12), line(13), line(14), line(15), line(16), line(17)).map(_.toDouble)))
        ),
        // Actual Data
        Array(
          line(8),
          line(9),
          line(11),
          line(12),
          line(13),
          line(14),
          line(15),
          line(16),
          line(17)
        )
      )
    }
    // Group every player
    val nbaGroups = nbaPlayers.groupBy{
      player => player(0)(4)
    }.collect()
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
    val nbaPlayers2014ByGroup = nba2014.map{
      line => Array(
        // Metadata
        Array(
          // ID
          line(0),
          // Name
          line(1),
          // Season
          line(2),
          // Age
          line(3),
          // Computed Group
          nbaClusterModel.predict(Vectors.dense(Array(line(8), line(9), line(11), line(12), line(13), line(14), line(15), line(16), line(17)).map(_.toDouble)))
        ),
        // Actual Data
        Array(
          line(8),
          line(9),
          line(11),
          line(12),
          line(13),
          line(14),
          line(15),
          line(16),
          line(17)
        )
      )
    }.groupBy{
      // Predict using the actual data
      player => player(0)(4)
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
              .reduce((similarPlayerId1, similarPlayerId2) => similarPlayerId1.toString + ";" + similarPlayerId2.toString)
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
    println("Cost of the NBA Model: %s".format(nbaClusterModel.computeCost(trainingNbaData)))

    // Create a Document to represent the data
    // Print the average stat for each group
    printToFile(new File(nbaCentersOutput)) {
      p => {
        p.println("fg,3pm,ft,reb,ast,stl,blk,tov,pts")
        nbaClusterCenter.foreach(center => p.println("%s,%s,%s,%s,%s,%s,%s,%s,%s"
          .format(center(0), center(1), center(2), center(3), center(4), center(5), center(6), center(7), center(8))))
      }
    }
    printToFile(new File(nbaPlayers2014Output)) {
      p => {
        p.println("cluster,player,playerId,fg,3pm,ft,reb,ast,stl,blk,tov,pts,age,similarPlayers")
        for((group, players) <- nbaGroups) {
          players.foreach(player => p.println("%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s,%s".format(
            group,
            player(0)(1),
            player(0)(0),
            player(1)(0).toString.toDouble,
            player(1)(1).toString.toDouble,
            player(1)(2).toString.toDouble,
            player(1)(3).toString.toDouble,
            player(1)(4).toString.toDouble,
            player(1)(5).toString.toDouble,
            player(1)(6).toString.toDouble,
            player(1)(7).toString.toDouble,
            player(1)(8).toString.toDouble,
            player(0)(3),
            nbaPlayers
              .filter(
                line => line(0)(4).equals(group)
                  && line(0)(3).toString.toInt >= player(0)(3).toString.toInt - 1
                  && line(0)(3).toString.toInt <= player(0)(3).toString.toInt + 1)
              .map(similarPlayer => similarPlayer(0)(0))
              .reduce((similarPlayerId1, similarPlayerId2) => similarPlayerId1.toString + ";" + similarPlayerId2.toString)
          )))
        }
      }
    }
    printToFile(new File(nbaPlayersHistoricalOutput)) {
      p => {
        p.println("cluster,player,playerId,fg,3pm,ft,reb,ast,stl,blk,tov,pts,age")
        for((group, players) <- nbaGroups) {
          players.foreach(player => p.println("%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s".format(
            group,
            player(0)(1),
            player(0)(0),
            player(1)(0).toString.toDouble,
            player(1)(1).toString.toDouble,
            player(1)(2).toString.toDouble,
            player(1)(3).toString.toDouble,
            player(1)(4).toString.toDouble,
            player(1)(5).toString.toDouble,
            player(1)(6).toString.toDouble,
            player(1)(7).toString.toDouble,
            player(1)(8).toString.toDouble,
            player(0)(3))))
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


