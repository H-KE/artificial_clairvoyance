package com.artificialclairvoyance.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{PolynomialExpansion, VectorAssembler}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import java.io._

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.{DataFrame, functions, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Top level application container.
 * This will set up the necessary context and invoke our application
 * TODO: this is just a stub right now
 **/
object ArtificialClairvoyance {
  val toDouble = udf[Double, String]( _.toDouble)
  val toInt    = udf[Int, String]( _.toInt)

  // Input Locations
  val battingFile = "src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv"
  val nbaFile = "src/test/resources/nba/nbaPlayerTotals.csv"


  // Output Locations
  val mlbCentersOutput = "app/resources/output/mlb_centers.csv"
  val mlbPlayersHistoricalOutput = "app/resources/output/mlb_players_historical.csv"
  val mlbPlayers2014Output = "app/resources/output/mlb_players2014.csv"
  val nbaCentersOutput = "app/resources/output/nba_centers.csv"
  val nbaPlayersHistoricalOutput = "app/resources/output/nba_players_historical.csv"
  val nbaPlayers2014Output = "app/resources/output/nba_players2014.csv"

  def main(args: Array[String]) {
    // Set up spark context
    val conf = new SparkConf().setAppName("Artificial Clairvoyance")
    val sc = new SparkContext(conf)

    /**
     * Input collected data
     * TODO: The filepath should not be hard coded
     */
    /* mlb */
    // Inputs
    val rawBattingData = sc.textFile(battingFile, 2).cache()

    /* nba */
    // Inputs
    val rawNbaData = sc.textFile(nbaFile, 2).cache()

    val clusteredMlbPlayers = mlbClustering(sc, battingFile, 1980, 100, 20, 5000)
    clusteredMlbPlayers
      .select("Cluster", "PlayerId", "Age", "Season", "Games", "HR", "H")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/mlb_players_historical2.csv")

    val clusteredNbaPlayers = nbaClustering(sc, nbaFile, 1980, 40, 40, 5000)
    clusteredNbaPlayers
      .select("Cluster", "PlayerId", "Name", "Age", "Season", "Games", "PTS", "AST", "REB", "STL", "BLK", "TOV", "3PM", "FG%", "3P%", "FT%")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/nba_players_historical2.csv")


//    /**
//     * Parse the necessary data from the collected data.
//     * Get it ready for the machine learning algorithm
//     * TODO: Need to abstract the parsing in a different object
//     */
//    /* mlb */
//    // Only get data for 2014 and with games played greater than 100 (to reduce noise)
//    // Used for prediction
//    val batters2014 = rawBattingData.map(_.split(","))
//      .filter(line => line(1).forall(_.isDigit) && line(1).toInt.equals(2014) && line(5).toInt >= 100)
//    // Get historical data for mlb. This is what we will use to train our models
//    val battersHistorical = rawBattingData.map(_.split(","))
//      .filter(line => line(1).forall(_.isDigit) && line(1).toInt >= 1980 && line(1).toInt <= 2014 && line(5).toInt >= 100)
//    // parsedData contains the metrics we are interested in, in vector form for each player.
//    val trainingBattingData = battersHistorical.map {
//      line => Vectors.dense(
//        line(8).toDouble,
//        line(11).toDouble
//      )
//    }
//
//    /* nba */
//    // Only get data for 2014 and with games played greater than 40
//    val nba2014 = rawNbaData.map(_.split(","))
//      .filter(line => line(2).forall(_.isDigit) && line(2).toInt.equals(2015) && line(6).toDouble >= 40)
//      .map(line => Array(
//        line(0),// (0)PlayerId,
//        line(1),// (1)Name,
//        line(2),// (2)Season,
//        line(3),// (3)Age,
//        line(4),// (4)Position,
//        line(5),// (5)Team,
//        line(6),// (6)Games,
//        line(7),// (7)MP,
//        line(8),// (8)FG%,
//        (line(9).toDouble/line(6).toDouble).toString,// (9)3PM per game,
//        line(10),// (10)3P%,
//        line(11),// (11)FT%,
//        (line(12).toDouble/line(6).toDouble).toString,// (12)REB per game,
//        (line(13).toDouble/line(6).toDouble).toString,// (13)AST per game,
//        (line(14).toDouble/line(6).toDouble).toString,// (14)STL per game,
//        (line(15).toDouble/line(6).toDouble).toString,// (15)BLK per game,
//        (line(16).toDouble/line(6).toDouble).toString,// (16)TOV per game,
//        (line(17).toDouble/line(6).toDouble).toString// (17)PTS per game
//      ))
//    // Get historical data for nba. This is what we will use to train our models
//    val nbaHistorical = rawNbaData.map(_.split(","))
//      .filter(line => line(2).forall(_.isDigit) && line(6).toDouble >= 20)
//      .map(line => Array(
//        line(0),// (0)PlayerId,
//        line(1),// (1)Name,
//        line(2),// (2)Season,
//        line(3),// (3)Age,
//        line(4),// (4)Position,
//        line(5),// (5)Team,
//        line(6),// (6)Games,
//        line(7),// (7)MP,
//        line(8),// (8)FG%,
//        (line(9).toDouble/line(6).toDouble).toString,// (9)3PM per game,
//        line(10),// (10)3P%,
//        line(11),// (11)FT%,
//        (line(12).toDouble/line(6).toDouble).toString,// (12)REB per game,
//        (line(13).toDouble/line(6).toDouble).toString,// (13)AST per game,
//        (line(14).toDouble/line(6).toDouble).toString,// (14)STL per game,
//        (line(15).toDouble/line(6).toDouble).toString,// (15)BLK per game,
//        (line(16).toDouble/line(6).toDouble).toString,// (16)TOV per game,
//        (line(17).toDouble/line(6).toDouble).toString// (17)PTS per game
//      ))
//    // use FG%(8), 3PM(9), FT%(11), REB(12), AST(13), STL(14), BLK(15), TOV(16), PTS(17)
//    val trainingNbaData = nbaHistorical.map {
//      line => Vectors.dense(
//        line(8).toDouble,
//        line(9).toDouble,
//        line(11).toDouble,
//        line(12).toDouble,
//        line(13).toDouble,
//        line(14).toDouble,
//        line(15).toDouble,
//        line(16).toDouble,
//        line(17).toDouble
//      )
//    }
//
//    /**
//     * Clustering algorithm. Cluster every seasonal performance by every player.
//     * This will return a model that can give the type of seasonal performance,
//     * and the grouping of every seasonal performance.
//     * TODO: Abstract the machine learning portion
//     */
//    // Cluster using K-means
//    /* mlb */
//    val iterationCountMLB = 1000
//    val clusterCountMLB = 20
//    // Produce the MLB clustering model
//    val mlbClusterModel = KMeans.train(trainingBattingData, clusterCountMLB, iterationCountMLB)
//    // Find centers of each cluster
//    val mlbClusterCenter = mlbClusterModel.clusterCenters map (_.toArray)
//    val mlbPlayers = battersHistorical.map{
//      line => Array(
//        // Metadata
//        Array(
//          // ID
//          line(0),
//          // Season
//          line(1),
//          // Age
//          line(22),
//          // Group, Predict using the model
//          mlbClusterModel.predict(Vectors.dense(Array(line(8),line(11)).map(_.toDouble)))
//        ),
//        // Actual data
//        Array(
//          // Hit
//          line(8),
//          // HR
//          line(11)
//        )
//      )
//    }
//    // Group every player
//    val mlbGroups = mlbPlayers.groupBy{
//      // group by cluster ID
//      player => player(0)(3)
//    }.collect()
//
//
//    /* nba */
//    val iterationCountNBA = 10000
//    val clusterCountNBA = 40
//    // Produce the NBA clustering model
//    val nbaClusterModel = KMeans.train(trainingNbaData, clusterCountNBA, iterationCountNBA)
//    // Find centers of each cluster
//    val nbaClusterCenter = nbaClusterModel.clusterCenters map (_.toArray)
//    val nbaPlayers = nbaHistorical.map{
//      line => Array(
//        // Metadata
//        Array(
//          // ID
//          line(0),
//          // Name
//          line(1),
//          // Season
//          line(2),
//          // Age
//          line(3),
//          // Computed Group
//          nbaClusterModel.predict(Vectors.dense(Array(line(8), line(9), line(11), line(12), line(13), line(14), line(15), line(16), line(17)).map(_.toDouble)))
//        ),
//        // Actual Data
//        Array(
//          line(8),
//          line(9),
//          line(11),
//          line(12),
//          line(13),
//          line(14),
//          line(15),
//          line(16),
//          line(17)
//        )
//      )
//    }
//    // Group every player
//    val nbaGroups = nbaPlayers.groupBy{
//      player => player(0)(4)
//    }.collect()
//
//    /**
//     * Matching algorithm. Given current players and their past seasonal performances,
//     * we match the player's most recent season(s) with a cluster from the previous step.
//     * Then we find players from that cluster who's had a similar age (e.g. +/- 1yrs?) when they had this season type.
//     * Return the current players and their corresponding list of "similar players"
//     * TODO: Find the list of players of that cluster with similar age
//     * TODO: Abstract this out
//     */
//    /* mlb */
//    val mlbPlayers2014ByGroup = batters2014.map{
//      line => Array(
//        // Metadata
//        Array(
//          // ID
//          line(0),
//          // Season
//          line(1),
//          // Age
//          line(22),
//          // Computed Group
//          mlbClusterModel.predict(Vectors.dense(Array(line(8),line(11)).map(_.toDouble)))
//        ),
//        // Actual data
//        Array(
//          // Hit
//          line(8),
//          // HR
//          line(11)
//        )
//      )
//    }.groupBy{
//      // Group by the group for later uses
//      player => player(0)(3)
//    }.collect()
//
//    /* nba */
//    val nbaPlayers2014ByGroup = nba2014.map{
//      line => Array(
//        // Metadata
//        Array(
//          // ID
//          line(0),
//          // Name
//          line(1),
//          // Season
//          line(2),
//          // Age
//          line(3),
//          // Computed Group
//          nbaClusterModel.predict(Vectors.dense(Array(line(8), line(9), line(11), line(12), line(13), line(14), line(15), line(16), line(17)).map(_.toDouble)))
//        ),
//        // Actual Data
//        Array(
//          line(8),
//          line(9),
//          line(11),
//          line(12),
//          line(13),
//          line(14),
//          line(15),
//          line(16),
//          line(17)
//        )
//      )
//    }.groupBy{
//      // Predict using the actual data
//      player => player(0)(4)
//    }.collect()
//
//    /**
//     * Output the results so far
//     * TODO: Print for now, but save it to some output file when it's ready
//     * TODO: optional... visualize the data (for 2-d data)
//     */
//    /* mlb */
//    // Print total cost
//    println("Cost of the MLB Model: %s".format(mlbClusterModel.computeCost(trainingBattingData)))
//
//    // Create a Document to represent the data
//    // Print the average stat for each group
//    printToFile(new File(mlbCentersOutput)) {
//      p => {
//        p.println("hits,homeruns")
//        mlbClusterCenter.foreach(center => p.println("%s,%s".format(center(0), center(1))))
//      }
//    }
//    printToFile(new File(mlbPlayers2014Output)) {
//      p => {
//        p.println("cluster,playerId,hits,homeruns,age,similarPlayers")
//        for((group, players) <- mlbPlayers2014ByGroup) {
//          players.foreach(player => p.println("%s,%s,%s,%s,%s,%s".format(
//            group,
//            player(0)(0),
//            player(1)(0),
//            player(1)(1),
//            player(0)(2),
//            mlbPlayers
//              .filter(
//                line => line(0)(3).equals(group)
//                && line(0)(2).toString.toInt >= player(0)(2).toString.toInt - 1
//                && line(0)(2).toString.toInt <= player(0)(2).toString.toInt + 1)
//              .map(similarPlayer => similarPlayer(0)(0))
//              .reduce((similarPlayerId1, similarPlayerId2) => similarPlayerId1.toString + ";" + similarPlayerId2.toString)
//          )))
//        }
//      }
//    }
//    printToFile(new File(mlbPlayersHistoricalOutput)) {
//      p => {
//        p.println("cluster,historicalPlayerId,season,hits,homeruns,historicalAge")
//        for((group, players) <- mlbGroups) {
//          players.foreach(player => p.println("%s,%s,%s,%s,%s,%s".format(group, player(0)(0),player(0)(1), player(1)(0), player(1)(1), player(0)(2))))
//        }
//      }
//    }
//
//    /* nba */
//    // Print total cost
//    println("Cost of the NBA Model: %s".format(nbaClusterModel.computeCost(trainingNbaData)))
//
//    // Create a Document to represent the data
//    // Print the average stat for each group
//    printToFile(new File(nbaCentersOutput)) {
//      p => {
//        p.println("fg,3pm,ft,reb,ast,stl,blk,tov,pts")
//        nbaClusterCenter.foreach(center => p.println("%s,%s,%s,%s,%s,%s,%s,%s,%s"
//          .format(center(0), center(1), center(2), center(3), center(4), center(5), center(6), center(7), center(8))))
//      }
//    }
//    printToFile(new File(nbaPlayers2014Output)) {
//      p => {
//        p.println("cluster,player,playerId,fg,3pm,ft,reb,ast,stl,blk,tov,pts,age,similarPlayers")
//        for((group, players) <- nbaPlayers2014ByGroup) {
//          players.foreach(player => p.println("%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s,%s".format(
//            group,
//            player(0)(1),
//            player(0)(0),
//            player(1)(0).toString.toDouble,
//            player(1)(1).toString.toDouble,
//            player(1)(2).toString.toDouble,
//            player(1)(3).toString.toDouble,
//            player(1)(4).toString.toDouble,
//            player(1)(5).toString.toDouble,
//            player(1)(6).toString.toDouble,
//            player(1)(7).toString.toDouble,
//            player(1)(8).toString.toDouble,
//            player(0)(3),
//            nbaPlayers
//              .filter(
//                line => line(0)(4).equals(group)
//                  && line(0)(3).toString.toInt >= player(0)(3).toString.toInt - 1
//                  && line(0)(3).toString.toInt <= player(0)(3).toString.toInt + 1)
//              .map(similarPlayer => similarPlayer(0)(0))
//              .reduce((similarPlayerId1, similarPlayerId2) => similarPlayerId1.toString + ";" + similarPlayerId2.toString)
//          )))
//        }
//      }
//    }
//    printToFile(new File(nbaPlayersHistoricalOutput)) {
//      p => {
//        p.println("cluster,historicalPlayer,historicalPlayerId,season,fg,3pm,ft,reb,ast,stl,blk,tov,pts,historicalAge")
//        for((group, players) <- nbaGroups) {
//          players.foreach(player => p.println("%s,%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s".format(
//            group,
//            player(0)(1),
//            player(0)(0),
//            player(0)(2),
//            player(1)(0).toString.toDouble,
//            player(1)(1).toString.toDouble,
//            player(1)(2).toString.toDouble,
//            player(1)(3).toString.toDouble,
//            player(1)(4).toString.toDouble,
//            player(1)(5).toString.toDouble,
//            player(1)(6).toString.toDouble,
//            player(1)(7).toString.toDouble,
//            player(1)(8).toString.toDouble,
//            player(0)(3))))
//        }
//      }
//    }
//
//
//
//    /**
//     * Regression Algorithm.
//     * Create a predictive model of each player from historical careers of their "similar players"
//     */
//    regression(sc, nbaPlayers2014Output, nbaPlayersHistoricalOutput, "app/resources/output/nba_predictions", "pts")
//    //regression(sc, mlbPlayers2014Output, mlbPlayersHistoricalOutput, "app/resources/output/mlb_players2014_models_poly.csv")
//
    // Terminate the spark context
    sc.stop()
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /********************************************************************************************************************
   *
   *  CLUSTERING ALGORITHMS
   *
   ********************************************************************************************************************/

  /**
   * Clustering function for MLB data
   * @param sc
   * @param rawFile
   * @param seasonFrom
   * @param minNumGames
   * @param numClusters
   * @param iterations
   * @return
   */
  def mlbClustering (sc:SparkContext, rawFile:String, seasonFrom:Int, minNumGames:Int, numClusters:Int, iterations:Int): DataFrame ={
    val rawData = preprocessData(sc, rawFile, seasonFrom, minNumGames)

    // Further preprocess of data
    val historicalPlayers = rawData
      // Homeruns
      .withColumn("HRtmp", toDouble(rawData("HR")))
      .drop("HR")
      .withColumnRenamed("HRtmp", "HR")
      // Hits
      .withColumn("Htmp", toDouble(rawData("H")))
      .drop("H")
      .withColumnRenamed("Htmp", "H")

    // Cluster all players
    createClusterModelAndClusterPlayers(historicalPlayers, numClusters, iterations, Array("HR", "H"))
  }

  /**
   * Clustering function for NBA data
   * @param sc
   * @param rawFile
   * @param seasonFrom
   * @param minNumGames
   * @param numClusters
   * @param iterations
   * @return
   */
  def nbaClustering (sc:SparkContext, rawFile:String, seasonFrom:Int, minNumGames:Int, numClusters:Int, iterations:Int): DataFrame ={
    val rawData = preprocessData(sc, rawFile, seasonFrom, minNumGames)

    // Further preprocess of data
    val historicalPlayers = rawData
      // Points per game
      .withColumn("PTStmp", toDouble(rawData("PTS"))/rawData("Games"))
      .drop("PTS")
      .withColumnRenamed("PTStmp", "PTS")
      // Assists per game
      .withColumn("ASTtmp", toDouble(rawData("AST"))/rawData("Games"))
      .drop("AST")
      .withColumnRenamed("ASTtmp", "AST")
      // Rebounds per game
      .withColumn("REBtmp", toDouble(rawData("REB"))/rawData("Games"))
      .drop("REB")
      .withColumnRenamed("REBtmp", "REB")
      // Steals per game
      .withColumn("STLtmp", toDouble(rawData("STL"))/rawData("Games"))
      .drop("STL")
      .withColumnRenamed("STLtmp", "STL")
      // Blocks per game
      .withColumn("BLKtmp", toDouble(rawData("BLK"))/rawData("Games"))
      .drop("BLK")
      .withColumnRenamed("BLKtmp", "BLK")
      // Turnovers per game
      .withColumn("TOVtmp", toDouble(rawData("TOV"))/rawData("Games"))
      .drop("TOV")
      .withColumnRenamed("TOVtmp", "TOV")
      // 3PM per game
      .withColumn("3PMtmp", toDouble(rawData("3PM"))/rawData("Games"))
      .drop("3PM")
      .withColumnRenamed("3PMtmp", "3PM")
      // FG%
      .withColumn("FG%tmp", toDouble(rawData("FG%")))
      .drop("FG%")
      .withColumnRenamed("FG%tmp", "FG%")
      // 3P%
      .withColumn("3P%tmp", toDouble(rawData("3P%")))
      .drop("3P%")
      .withColumnRenamed("3P%tmp", "3P%")
      // FT%
      .withColumn("FT%tmp", toDouble(rawData("FT%")))
      .drop("FT%")
      .withColumnRenamed("FT%tmp", "FT%")

    // Cluster all players
    createClusterModelAndClusterPlayers(historicalPlayers, numClusters, iterations, Array("PTS", "AST", "REB", "STL", "BLK", "TOV", "3PM", "FG%", "3P%", "FT%"))
  }

  def preprocessData (sc:SparkContext, rawFile:String, seasonFrom:Int, minNumGames:Int): DataFrame ={
    val sqlContext = new SQLContext(sc)

    // Read in the input file
    val rawData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(rawFile).na.drop()

    val historicalPlayers = rawData
      // Filter Seasons
      .withColumn("SeasonTmp", toInt(rawData("Season")))
      .drop("Season")
      .withColumnRenamed("SeasonTmp", "Season")
      .filter("Season >= " + seasonFrom)
      // Filter Games (Noise reduction)
      .withColumn("GamesTmp", toInt(rawData("Games")))
      .drop("Games")
      .withColumnRenamed("GamesTmp", "Games")
      .filter("Games > " + minNumGames)
      .withColumn("Agetmp", toInt(rawData("Age")))
      .drop("Age")
      .withColumnRenamed("Agetmp", "Age")

    historicalPlayers
  }

  def createClusterModelAndClusterPlayers(historicalPlayers:DataFrame, numClusters:Int, iterations:Int, stats:Array[String]): DataFrame = {
    // Assemble the stats of interest
    val assembler = new VectorAssembler()
      .setInputCols(stats)
      .setOutputCol("clusterVector")
    val historicalPlayersWithClusterVectors = assembler.transform(historicalPlayers)

    // Train the cluster model
    val trainingVectors = historicalPlayersWithClusterVectors
      .select("clusterVector").rdd
      .map(vector => vector(0).asInstanceOf[Vector])
    val clusterModel = KMeans.train(trainingVectors, numClusters, iterations)

    // Find the cluster for each player
    val findCluster = udf((x:Vector) => clusterModel.predict(x))
    val clusteredHistoricalPlayers = historicalPlayersWithClusterVectors
      .withColumn("Cluster", findCluster(historicalPlayersWithClusterVectors("clusterVector")))

    clusteredHistoricalPlayers.drop("clusterVector")
  }

  /********************************************************************************************************************
    *
    *  MATCHING ALGORITHM
    *
    ********************************************************************************************************************/
  /**
   * Main matching algorithm
   * @param historicalPlayers
   * @param finalSeason
   * @return
   */
  def matchCurrentPlayers(historicalPlayers:DataFrame, finalSeason:Int): DataFrame = {
    val currentPlayers = historicalPlayers.filter("Season = " + finalSeason)

    val similarPlayerHistory = currentPlayers
      .select("playerId", "Age")
      .withColumnRenamed("playerId", "currentPlayerId")
      .withColumnRenamed("Age", "currentPlayerAge")
      .join(
        historicalPlayers,
        currentPlayers("similarPlayer")===historicalPlayers("historicalPlayerId"),
        "left")
      .na.drop()
////            nbaPlayers
//                .filter(
//                  line => line(0)(4).equals(group)
//                    && line(0)(3).toString.toInt >= player(0)(3).toString.toInt - 1
//                    && line(0)(3).toString.toInt <= player(0)(3).toString.toInt + 1)
  }

  /********************************************************************************************************************
    *
    *  REGRESSION ALGORITHM
    *
    ********************************************************************************************************************/
  /**
   * General regression function
   * @param sc
   * @param currentFile
   * @param historicalFile
   * @param outputDirectory
   * @param statToPredict
   */
  def regression(sc:SparkContext, currentFile:String, historicalFile:String, outputDirectory:String, statToPredict:String): Unit ={
    val sqlContext = new SQLContext(sc)

    // Read in historical data into rdd/df
    val historicalPlayers = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(historicalFile).na.drop()
    val currentPlayers = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(currentFile)
      .explode("similarPlayers", "similarPlayer") {player: String => player.split(";")}
      .drop("similarPlayers")
      .na.drop()

    // Join the historical players with the current players
    val similarPlayerHistory = currentPlayers
      .select("playerId", "age", "similarPlayer")
      .join(
        historicalPlayers,
        currentPlayers("similarPlayer")===historicalPlayers("historicalPlayerId"),
        "left")
      .na.drop()

    // Convert age column to double type in data frame
    // Also do polynomial expansion & normalize
    // Equation for 3rd degree poly would look like (x+ (x^2)/10^2 + (X^3)/10^3 )
    val squareNormed = udf((x:Double) => math.pow(x, 2)/100)
    val cubeNormed = udf((x:Double) => math.pow(x, 3)/1000)
    val fourthNormed = udf((x:Double) => math.pow(x, 4)/10000)
    // val fifthNormed = udf((x:Double) => math.pow(x, 5)/100000)
    val similarPlayerHistoryPoly = similarPlayerHistory
      .withColumn("historicalAgeDouble", toDouble(similarPlayerHistory("historicalAge")))
      .withColumn("historicalAgeDouble2", squareNormed(similarPlayerHistory("historicalAge")))
      .withColumn("historicalAgeDouble3", cubeNormed(similarPlayerHistory("historicalAge")))
      .withColumn("historicalAgeDouble4", fourthNormed(similarPlayerHistory("historicalAge")))
      // .withColumn("historicalAgeDouble5", fifthNormed(similarPlayerHistory("historicalAge")))
      .drop("historicalAge")

    // Convert age column into vector.
    val assembler = new VectorAssembler()
      .setInputCols(Array("historicalAgeDouble", "historicalAgeDouble2", "historicalAgeDouble3", "historicalAgeDouble4"))
      .setOutputCol("ageVector")
    val allSimilarPlayerHistory = assembler.transform(similarPlayerHistoryPoly)
    allSimilarPlayerHistory.orderBy("playerId").show(100)

    // Convert back to RDD for ease of use
    val allSimilarPlayers_RDD = allSimilarPlayerHistory
      .select("playerId", "age", statToPredict, "ageVector")
      .rdd
    // Group historical data by player
    val grouped_allSimilarPlayers = allSimilarPlayers_RDD.groupBy{
      player => player(0)
    }.collect()

    // loop through each current player
    // TODO: Make this parallel, needs to aggregate better
    for((player, similarPlayers) <- grouped_allSimilarPlayers) {

      //similarPlayers is an iterable, convert to RDD
      val similarPlayers_RDD = sc.parallelize(similarPlayers.toList)
      val labeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(2).toString.toDouble, parts(3).asInstanceOf[Vector])
      }

      //create regression object
      val regression = new LinearRegressionWithSGD().setIntercept(true)
      regression.optimizer.setStepSize(0.001)
      regression.optimizer.setNumIterations(3000)
      //run the regression
      val model = regression.run(labeledPoints)

      // Save Prediction & Weights
      val playerSample = similarPlayers.toList.head
      val age = playerSample.getString(1).toDouble
      val array = Array(age, math.pow(age, 2)/100, math.pow(age, 3)/1000, math.pow(age, 4)/10000)
      printToFile(new File(outputDirectory + "/" + statToPredict + "_" + player)) {
        p => {
          p.println("playerId," + statToPredict + ",intercept,weights")
          p.println("%s,%s,%s,%s,%s,%s,%s".format(
            player,
            model.predict(Vectors.dense(array)).toString,
            model.intercept.toString,
            model.weights(0).toString,
            model.weights(1).toString,
            model.weights(2).toString,
            model.weights(3).toString
          ))
        }
      }
    }
  }
}


