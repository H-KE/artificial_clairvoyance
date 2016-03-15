package com.artificialclairvoyance.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{PolynomialExpansion, VectorAssembler}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import java.io._

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.{GroupedData, DataFrame, functions, SQLContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

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
    val clusteredMlbPlayers = mlbClustering(sc, battingFile, 1980, 100, 20, 5000, mlbCentersOutput)
    clusteredMlbPlayers
      .select("Cluster", "PlayerId", "Age", "Season", "Games", "HR", "H")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/mlb_players_historical")
    val matchedCurrentMlbPlayers = matchCurrentPlayers(clusteredMlbPlayers, 2014)
    matchedCurrentMlbPlayers
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/mlb_players_current")
    val mlbPredictionGrouped = nonRegressionPrediction(matchedCurrentMlbPlayers, clusteredMlbPlayers)
    mlbPredictionGrouped
      .agg(avg("HR"), avg("H"))
      .withColumnRenamed("CurrentPlayerId", "PlayerId")
      .withColumnRenamed("avg(H)", "H")
      .withColumnRenamed("avg(HR)", "HR")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/mlb_players_predictions")

//    val mlbPrediction = mlbRegression(sc, matchedCurrentMlbPlayers, clusteredMlbPlayers)
//    printToFile(new File("app/resources/output/mlb_predictions.csv")) {
//      p => {
//        p.println("PlayerId,Age,HR,H")
//        mlbPrediction.foreach(line =>
//          p.println("%s,%s,%s,%s"
//            .format(
//              line(0).toString,
//              line(1).toString,
//              line(2).toString,
//              line(3).toString
//            )
//          )
//        )
//      }
//    }

    /**
     * NBA
     */
    val clusteredNbaPlayers = nbaClustering(sc, nbaFile, 1980, 65, 50, 10000, nbaCentersOutput)
    clusteredNbaPlayers
      .select("Cluster", "PlayerId", "Name", "Age", "Season", "Games", "PTS", "AST", "REB")//, "STL", "BLK", "TOV", "3PM", "FG%", "3P%", "FT%")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/nba_players_historical")

    val matchedCurrentNbaPlayers = matchCurrentPlayers(clusteredNbaPlayers, 2015)
    matchedCurrentNbaPlayers
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/nba_players_current")
    val nbaPredictionGrouped = nonRegressionPrediction(matchedCurrentNbaPlayers, clusteredNbaPlayers)
    nbaPredictionGrouped
      .agg(avg("PTS"), avg("AST"), avg("REB"))
      .withColumnRenamed("CurrentPlayerId", "PlayerId")
      .withColumnRenamed("avg(PTS)", "PTS")
      .withColumnRenamed("avg(AST)", "AST")
      .withColumnRenamed("avg(REB)", "REB")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("app/resources/output/nba_players_predictions")


//    val nbaPrediction = nbaRegression(sc, matchedCurrentNbaPlayers, clusteredNbaPlayers)
//    printToFile(new File("app/resources/output/nba_predictions.csv")) {
//      p => {
//        p.println("PlayerId,Age,PTS,AST,REB")//,STL,BLK,TOV,3PM,FG%,3P%,FT%")
//        nbaPrediction.foreach(line =>
//          p.println("%s,%s,%s,%s,%s"//,%s,%s,%s,%s,%s,%s,%s"
//            .format(
//              line(0).toString,
//              line(1).toString,
//              line(2).toString,
//              line(3).toString,
//              line(4).toString//,
////              line(5).toString,
////              line(6).toString,
////              line(7).toString,
////              line(8).toString,
////              line(9).toString,
////              line(10).toString,
////              line(11).toString
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
  def mlbClustering (sc:SparkContext, rawFile:String, seasonFrom:Int, minNumGames:Int, numClusters:Int, iterations:Int, outputFile:String): DataFrame ={
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
    createClusterModelAndClusterPlayers(historicalPlayers, numClusters, iterations, Array("HR", "H"), outputFile)
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
  def nbaClustering (sc:SparkContext, rawFile:String, seasonFrom:Int, minNumGames:Int, numClusters:Int, iterations:Int, outputFile:String): DataFrame ={
    val rawRawData = preprocessData(sc, rawFile, seasonFrom, minNumGames)
    // filter out people who don't play enough (noise reduction)
    val mpDouble = rawRawData
      .withColumn("MPtmp", toDouble(rawRawData("MP")))
      .drop("MP")
      .withColumnRenamed("MPtmp", "MP")
    val rawData = mpDouble
      .filter(mpDouble("MP") > mpDouble("Games")*15)

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
      // // Steals per game
      // .withColumn("STLtmp", toDouble(rawData("STL"))/rawData("Games"))
      // .drop("STL")
      // .withColumnRenamed("STLtmp", "STL")
      // // Blocks per game
      // .withColumn("BLKtmp", toDouble(rawData("BLK"))/rawData("Games"))
      // .drop("BLK")
      // .withColumnRenamed("BLKtmp", "BLK")
      // // Turnovers per game
      // .withColumn("TOVtmp", toDouble(rawData("TOV"))/rawData("Games"))
      // .drop("TOV")
      // .withColumnRenamed("TOVtmp", "TOV")
      // // 3PM per game
      // .withColumn("3PMtmp", toDouble(rawData("3PM"))/rawData("Games"))
      // .drop("3PM")
      // .withColumnRenamed("3PMtmp", "3PM")
      // // FG%
      // .withColumn("FG%tmp", toDouble(rawData("FG%")))
      // .drop("FG%")
      // .withColumnRenamed("FG%tmp", "FG%")
      // // 3P%
      // .withColumn("3P%tmp", toDouble(rawData("3P%")))
      // .drop("3P%")
      // .withColumnRenamed("3P%tmp", "3P%")
      // // FT%
      // .withColumn("FT%tmp", toDouble(rawData("FT%")))
      // .drop("FT%")
      // .withColumnRenamed("FT%tmp", "FT%")

    // Cluster all players
    createClusterModelAndClusterPlayers(historicalPlayers, numClusters, iterations, Array("PTS", "AST", "REB"), outputFile)//, "STL", "BLK", "TOV", "3PM", "FG%", "3P%", "FT%"), outputFile)
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

  def createClusterModelAndClusterPlayers(historicalPlayers:DataFrame, numClusters:Int, iterations:Int, stats:Array[String], outputFile:String): DataFrame = {
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

    //print centers to file for GUI
    val clusterCenter = clusterModel.clusterCenters map (_.toArray)
    printToFile(new File(outputFile)) {
      p => {
        p.println(stats.mkString(","))

        clusterCenter.foreach(center => p.println(center.mkString(",")))
      }
    }

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
        currentPlayers("CurrentPlayerCluster")===historicalPlayers("Cluster") && (
          // Current Player matches historical player's age
          currentPlayers("CurrentPlayerAge") === historicalPlayers("Age")
            // Variable age window for younger players
            || (currentPlayers("CurrentPlayerAge") < 24
            && currentPlayers("CurrentPlayerAge") <= historicalPlayers("Age") + 1
            && currentPlayers("CurrentPlayerAge") >= historicalPlayers("Age") - 1)
            // Variable age window for older players
            || (currentPlayers("CurrentPlayerAge") > 32
            && currentPlayers("CurrentPlayerAge") <= historicalPlayers("Age") + 1
            && currentPlayers("CurrentPlayerAge") >= historicalPlayers("Age") - 1)
          ),
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
  def nonRegressionPrediction(matchedCurrentPlayers:DataFrame, ClusteredHistoricalPlayers:DataFrame): GroupedData ={
    // Join the historical players with the current players
    val similarPlayerHistory = matchedCurrentPlayers
      .select("CurrentPlayerId", "CurrentPlayerAge", "SimilarPlayerId")
      .join(
        ClusteredHistoricalPlayers,
        matchedCurrentPlayers("SimilarPlayerId")===ClusteredHistoricalPlayers("PlayerId"),
        "left")
      .na.drop()
      .drop("PlayerId")

    // Filter to those with the current player's age + 1
    val simlarPlayerNextYear = similarPlayerHistory
      .filter(similarPlayerHistory("Age") === similarPlayerHistory("CurrentPlayerAge")+1)

    simlarPlayerNextYear
      .groupBy("CurrentPlayerId")
  }


  def mlbRegression(sc:SparkContext, matchedCurrentPlayers:DataFrame, ClusteredHistoricalPlayers:DataFrame): List[Array[Any]] ={
    val similarPlayerHistoryWithAgeVector = regressionPrep(sc, matchedCurrentPlayers, ClusteredHistoricalPlayers)

    // Convert back to RDD for ease of use
    val allSimilarPlayers_RDD = similarPlayerHistoryWithAgeVector
      .select("CurrentPlayerId", "CurrentPlayerAge", "HR", "H", "AgeVector")
      .orderBy("CurrentPlayerId")
      .rdd
    // Group historical data by player
    val grouped_allSimilarPlayers = allSimilarPlayers_RDD.groupBy{
      player => player(0)
    }.collect()

    val predictions = ListBuffer[Array[Any]]()

    // Loop through each current player
    for((player, similarPlayers) <- grouped_allSimilarPlayers) {
      // SimilarPlayers is an iterable, convert to RDD
      val similarPlayers_RDD = sc.parallelize(similarPlayers.toList)

      val hrLabeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(2).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      }.cache()

      val hitLabeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(3).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      }.cache()

      // Create regression object
      val hrRegression = new LinearRegressionWithSGD().setIntercept(true)
      hrRegression.optimizer.setStepSize(0.005)
      hrRegression.optimizer.setNumIterations(3000)
      // Run the regression
      val hrModel = hrRegression.run(hrLabeledPoints)

      // Create regression object
      val hitRegression = new LinearRegressionWithSGD().setIntercept(true)
      hitRegression.optimizer.setStepSize(0.005)
      hitRegression.optimizer.setNumIterations(3000)
      // Run the regression
      val hitModel = hitRegression.run(hitLabeledPoints)


      // Save Prediction & Weights
      val playerSample = similarPlayers.toList.head
      val age = playerSample.getInt(1).toDouble + 1
      val array = Array(age, math.pow(age, 2)/100)//, math.pow(age, 3)/1000)//, math.pow(age, 4)/10000)

      val prediction = Array(player, age, hrModel.predict(Vectors.dense(array)), hitModel.predict(Vectors.dense(array)))

      predictions += prediction
    }

    predictions.toList
  }

  def nbaRegression(sc:SparkContext, matchedCurrentPlayers:DataFrame, ClusteredHistoricalPlayers:DataFrame): List[Array[Any]] ={
    val similarPlayerHistoryWithAgeVector = regressionPrep(sc, matchedCurrentPlayers, ClusteredHistoricalPlayers)

    // Convert back to RDD for ease of use
    val allSimilarPlayers_RDD = similarPlayerHistoryWithAgeVector
      .select("CurrentPlayerId", "CurrentPlayerAge", "PTS", "AST", "REB", "AgeVector")//, "STL", "BLK", "TOV", "3PM", "FG%", "3P%", "FT%", "AgeVector")
      .orderBy("CurrentPlayerId")
      .rdd
    // Group historical data by player
    val grouped_allSimilarPlayers = allSimilarPlayers_RDD.groupBy{
      player => player(0)
    }.collect()

    val predictions = ListBuffer[Array[Any]]()

    // Loop through each current player
    for((player, similarPlayers) <- grouped_allSimilarPlayers) {
      // SimilarPlayers is an iterable, convert to RDD
      val similarPlayers_RDD = sc.parallelize(similarPlayers.toList)


      val ptsLabeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(2).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      }.cache()

      val astLabeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(3).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      }.cache()

      val rebLabeledPoints = similarPlayers_RDD.map { parts =>
        LabeledPoint(parts(4).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      }.cache()

      // val stlLabeledPoints = similarPlayers_RDD.map { parts =>
      //   LabeledPoint(parts(5).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      // }.cache()

      // val blkLabeledPoints = similarPlayers_RDD.map { parts =>
      //   LabeledPoint(parts(6).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      // }.cache()

      // val tovLabeledPoints = similarPlayers_RDD.map { parts =>
      //   LabeledPoint(parts(7).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      // }.cache()

      // val threeMadeLabeledPoints = similarPlayers_RDD.map { parts =>
      //   LabeledPoint(parts(8).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      // }.cache()

      // val fgLabeledPoints = similarPlayers_RDD.map { parts =>
      //   LabeledPoint(parts(9).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      // }.cache()

      // val threePerLabeledPoints = similarPlayers_RDD.map { parts =>
      //   LabeledPoint(parts(10).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      // }.cache()

      // val ftLabeledPoints = similarPlayers_RDD.map { parts =>
      //   LabeledPoint(parts(11).toString.toDouble, parts(parts.length-1).asInstanceOf[Vector])
      // }.cache()

      // Create regression object
      val ptsRegression = new LinearRegressionWithSGD().setIntercept(true)
      ptsRegression.optimizer.setStepSize(0.005)
      ptsRegression.optimizer.setNumIterations(3000)
      // Run the regression
      val ptsModel = ptsRegression.run(ptsLabeledPoints)

      // Create regression object
      val astRegression = new LinearRegressionWithSGD().setIntercept(true)
      astRegression.optimizer.setStepSize(0.005)
      astRegression.optimizer.setNumIterations(3000)
      // Run the regression
      val astModel = astRegression.run(astLabeledPoints)

      // Create regression object
      val rebRegression = new LinearRegressionWithSGD().setIntercept(true)
      rebRegression.optimizer.setStepSize(0.005)
      rebRegression.optimizer.setNumIterations(3000)
      // Run the regression
      val rebModel = rebRegression.run(rebLabeledPoints)

      // // Create regression object
      // val stlRegression = new LinearRegressionWithSGD().setIntercept(true)
      // stlRegression.optimizer.setStepSize(0.001)
      // stlRegression.optimizer.setNumIterations(3000)
      // // Run the regression
      // val stlModel = stlRegression.run(stlLabeledPoints)

      // // Create regression object
      // val blkRegression = new LinearRegressionWithSGD().setIntercept(true)
      // blkRegression.optimizer.setStepSize(0.001)
      // blkRegression.optimizer.setNumIterations(3000)
      // // Run the regression
      // val blkModel = blkRegression.run(blkLabeledPoints)

      // // Create regression object
      // val tovRegression = new LinearRegressionWithSGD().setIntercept(true)
      // tovRegression.optimizer.setStepSize(0.001)
      // tovRegression.optimizer.setNumIterations(3000)
      // // Run the regression
      // val tovModel = tovRegression.run(tovLabeledPoints)

      // // Create regression object
      // val threeMadeRegression = new LinearRegressionWithSGD().setIntercept(true)
      // threeMadeRegression.optimizer.setStepSize(0.001)
      // threeMadeRegression.optimizer.setNumIterations(3000)
      // // Run the regression
      // val threeMadeModel = threeMadeRegression.run(threeMadeLabeledPoints)

      // // Create regression object
      // val fgRegression = new LinearRegressionWithSGD().setIntercept(true)
      // fgRegression.optimizer.setStepSize(0.001)
      // fgRegression.optimizer.setNumIterations(3000)
      // // Run the regression
      // val fgPerModel = fgRegression.run(fgLabeledPoints)

      // // Create regression object
      // val threePerRegression = new LinearRegressionWithSGD().setIntercept(true)
      // threePerRegression.optimizer.setStepSize(0.001)
      // threePerRegression.optimizer.setNumIterations(3000)
      // // Run the regression
      // val threePerModel = threePerRegression.run(threePerLabeledPoints)

      // // Create regression object
      // val ftRegression = new LinearRegressionWithSGD().setIntercept(true)
      // ftRegression.optimizer.setStepSize(0.001)
      // ftRegression.optimizer.setNumIterations(3000)
      // // Run the regression
      // val ftModel = ftRegression.run(ftLabeledPoints)


      // Save Prediction & Weights
      val playerSample = similarPlayers.toList.head
      val age = playerSample.getInt(1).toDouble + 1
      val array = Array(age, math.pow(age, 2)/100)//, math.pow(age, 3)/1000)//, math.pow(age, 4)/10000)
      val prediction = Array(
        player,
        age,
        ptsModel.predict(Vectors.dense(array)),
        astModel.predict(Vectors.dense(array)),
        rebModel.predict(Vectors.dense(array))//,
        // stlModel.predict(Vectors.dense(array)),
        // blkModel.predict(Vectors.dense(array)),
        // tovModel.predict(Vectors.dense(array)),
        // threeMadeModel.predict(Vectors.dense(array)),
        // fgPerModel.predict(Vectors.dense(array)),
        // threePerModel.predict(Vectors.dense(array)),
        // ftModel.predict(Vectors.dense(array))
      )

      predictions += prediction
    }

    predictions.toList
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
    //val fourthNormed = udf((x:Double) => math.pow(x, 4)/10000)
    // val fifthNormed = udf((x:Double) => math.pow(x, 5)/100000)
    val similarPlayerHistoryFixed = similarPlayerHistory
      .withColumn("AgeTmp", toDouble(similarPlayerHistory("Age")))
      .drop("Age")
      .withColumnRenamed("AgeTmp", "Age")
    val similarPlayerHistoryPoly = similarPlayerHistoryFixed
      .withColumn("Age2", squareNormed(similarPlayerHistoryFixed("Age")))
    //  .withColumn("Age3", cubeNormed(similarPlayerHistoryFixed("Age")))
    //  .withColumn("Age4", fourthNormed(similarPlayerHistoryFixed("Age")))
    // .withColumn("Age5", fifthNormed(similarPlayerHistory("Age")))

    // Convert age column into vector.
    val assembler = new VectorAssembler()
      .setInputCols(Array("Age", "Age2"))//, "Age3"))//, "Age4"))
      .setOutputCol("AgeVector")
    val similarPlayerHistoryWithAgeVector = assembler.transform(similarPlayerHistoryPoly)
      .drop("Age").drop("Age2")//.drop("Age3")//.drop("Age4")

    similarPlayerHistoryWithAgeVector
  }
}


