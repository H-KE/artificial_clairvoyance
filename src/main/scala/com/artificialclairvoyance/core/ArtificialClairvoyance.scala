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
     * MLB
     */
    val clusteredMlbPlayers = mlbClustering(sc, battingFile, 1980, 100, 20, 5000)
    clusteredMlbPlayers
      .select("Cluster", "PlayerId", "Age", "Season", "Games", "HR", "H")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/mlb_players_historical2")

    val matchedCurrentMlbPlayers = matchCurrentPlayers(clusteredMlbPlayers, 2014)
    matchedCurrentMlbPlayers
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/mlb_players_current2")

//    val mlb_prediction = mlbRegression(sc, matchedCurrentMlbPlayers, clusteredMlbPlayers)

    /**
     * NBA
     */
    val clusteredNbaPlayers = nbaClustering(sc, nbaFile, 1980, 40, 40, 5000)
    clusteredNbaPlayers
      .select("Cluster", "PlayerId", "Name", "Age", "Season", "Games", "PTS", "AST", "REB", "STL", "BLK", "TOV", "3PM", "FG%", "3P%", "FT%")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/nba_players_historical2")

    val matchedCurrentNbaPlayers = matchCurrentPlayers(clusteredNbaPlayers, 2015)
    matchedCurrentNbaPlayers
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/nba_players_current2")

//    val nba_prediction = nbaRegression(sc, matchedCurrentNbaPlayers, clusteredNbaPlayers)
//    printToFile(new File("app/resources/output/nba_predictions.csv")) {
//      p => {
//        p.println("PlayerId,PTS")
//        nba_prediction.foreach(line =>
//          p.println("%s,%s"
//            .format(
//              line(0).toString,
//              line(1).toString
//            )
//          )
//        )
//      }
//    }


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
      .select("PlayerId", "Age", "Cluster")
      .withColumnRenamed("PlayerId", "CurrentPlayerId")
      .withColumnRenamed("Age", "CurrentPlayerAge")
      .withColumnRenamed("Cluster", "CurrentPlayerCluster")

    val similarPlayerHistory = currentPlayers
      .join(
        historicalPlayers,
        currentPlayers("CurrentPlayerCluster")===historicalPlayers("Cluster") &&
          currentPlayers("CurrentPlayerAge") <= historicalPlayers("Age") + 1 &&
          currentPlayers("CurrentPlayerAge") >= historicalPlayers("Age") - 1,
        "left")
      .na.drop()

    similarPlayerHistory
      .withColumnRenamed("PlayerId", "SimilarPlayerId")
      .withColumnRenamed("Age", "SimilarPlayerAge")
      .withColumnRenamed("Cluster", "SimilarPlayerCluster")
      .select("CurrentPlayerId", "CurrentPlayerAge", "SimilarPlayerId", "SimilarPlayerAge", "SimilarPlayerCluster")

  }

  /********************************************************************************************************************
    *
    *  REGRESSION ALGORITHM
    *
    ********************************************************************************************************************/
  def mlbRegression(sc:SparkContext, matchedCurrentPlayers:DataFrame, ClusteredHistoricalPlayers:DataFrame): List[Array[Any]] ={
    val similarPlayerHistoryWithAgeVector = regressionPrep(sc, matchedCurrentPlayers, ClusteredHistoricalPlayers)

    // Convert back to RDD for ease of use
    // TODO: Make this parallel, needs to aggregate better
    val allSimilarPlayers_RDD = similarPlayerHistoryWithAgeVector
      .rdd
    // Group historical data by player
    val grouped_allSimilarPlayers = allSimilarPlayers_RDD.groupBy{
      player => player(0)
    }.collect()

    val predictions = List()

    // Loop through each current player
    for((player, similarPlayers) <- grouped_allSimilarPlayers) {
      // SimilarPlayers is an iterable, convert to RDD
      val similarPlayers_RDD = sc.parallelize(similarPlayers.toList)
      val labeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(2).toString.toDouble, parts(3).asInstanceOf[Vector])
      }

      // Create regression object
      val regression = new LinearRegressionWithSGD().setIntercept(true)
      regression.optimizer.setStepSize(0.001)
      regression.optimizer.setNumIterations(3000)

      // Run the regression
      val model = regression.run(labeledPoints)

      // Save Prediction & Weights
      val playerSample = similarPlayers.toList.head
      val age = playerSample.getString(1).toDouble
      val array = Array(age, math.pow(age, 2)/100, math.pow(age, 3)/1000, math.pow(age, 4)/10000)

      predictions :+ Array(player, model.predict(Vectors.dense(array)))
    }

    predictions
  }

  def nbaRegression(sc:SparkContext, matchedCurrentPlayers:DataFrame, ClusteredHistoricalPlayers:DataFrame): List[Array[Any]] ={
    val similarPlayerHistoryWithAgeVector = regressionPrep(sc, matchedCurrentPlayers, ClusteredHistoricalPlayers)

    // Convert back to RDD for ease of use
    // TODO: Make this parallel, needs to aggregate better
    val allSimilarPlayers_RDD = similarPlayerHistoryWithAgeVector
      .rdd
    // Group historical data by player
    val grouped_allSimilarPlayers = allSimilarPlayers_RDD.groupBy{
      player => player(0)
    }.collect()

    val predictions = List()

    // Loop through each current player
    for((player, similarPlayers) <- grouped_allSimilarPlayers) {
      // SimilarPlayers is an iterable, convert to RDD
      val similarPlayers_RDD = sc.parallelize(similarPlayers.toList)
      val labeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(2).toString.toDouble, parts(3).asInstanceOf[Vector])
      }

      // Create regression object
      val regression = new LinearRegressionWithSGD().setIntercept(true)
      regression.optimizer.setStepSize(0.001)
      regression.optimizer.setNumIterations(3000)

      // Run the regression
      val model = regression.run(labeledPoints)

      // Save Prediction & Weights
      val playerSample = similarPlayers.toList.head
      val age = playerSample.getString(1).toDouble
      val array = Array(age, math.pow(age, 2)/100, math.pow(age, 3)/1000, math.pow(age, 4)/10000)

      predictions :+ Array(player, model.predict(Vectors.dense(array)))
    }

    predictions
  }

  /**
   * General Regression Algorithm
   * @param sc
   * @param matchedCurrentPlayers
   * @param ClusteredHistoricalPlayers
   */
  def regressionPrep(sc:SparkContext, matchedCurrentPlayers:DataFrame, ClusteredHistoricalPlayers:DataFrame): DataFrame ={

    // Join the historical players with the current players
    val similarPlayerHistory = matchedCurrentPlayers
      .select("CurrentPlayerId", "CurrentPlayerAge", "SimilarPlayerId")
      .join(
        ClusteredHistoricalPlayers,
        matchedCurrentPlayers("SimilarPlayerId")===ClusteredHistoricalPlayers("PlayerId"),
        "left")
      .na.drop()
      .drop("PlayerId")

    // Convert age column to double type in data frame
    // Also do polynomial expansion & normalize
    // Equation for 3rd degree poly would look like (x+ (x^2)/10^2 + (X^3)/10^3 )
    val squareNormed = udf((x:Double) => math.pow(x, 2)/100)
    val cubeNormed = udf((x:Double) => math.pow(x, 3)/1000)
    val fourthNormed = udf((x:Double) => math.pow(x, 4)/10000)
    // val fifthNormed = udf((x:Double) => math.pow(x, 5)/100000)
    val similarPlayerHistoryFixed = similarPlayerHistory
      .withColumn("AgeTmp", toDouble(similarPlayerHistory("Age")))
      .drop("Age")
      .withColumnRenamed("AgeTmp", "Age")
    val similarPlayerHistoryPoly = similarPlayerHistoryFixed
      .withColumn("Age2", squareNormed(similarPlayerHistoryFixed("Age")))
      .withColumn("Age3", cubeNormed(similarPlayerHistoryFixed("Age")))
      .withColumn("Age4", fourthNormed(similarPlayerHistoryFixed("Age")))
      // .withColumn("Age5", fifthNormed(similarPlayerHistory("Age")))

    // Convert age column into vector.
    val assembler = new VectorAssembler()
      .setInputCols(Array("Age", "Age2", "Age3", "Age4"))
      .setOutputCol("AgeVector")
    val similarPlayerHistoryWithAgeVector = assembler.transform(similarPlayerHistoryPoly)
      .drop("Age").drop("Age2").drop("Age3").drop("Age4")
    similarPlayerHistoryWithAgeVector.orderBy("CurrentPlayerId").show(100)

    similarPlayerHistoryWithAgeVector
  }
}


