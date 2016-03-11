package com.artificialclairvoyance.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,FloatType};

import ArtificialClairvoyance._

import java.io._
import scala.io._

object Regression extends Serializable{
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Regression")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val outputFile = "app/resources/output/mlb_playsers2014_models_poly.csv"
    //Homeruns mapped to age
    val currentData = "app/resources/output/mlb_players2014.csv"


    //read in historical data into rdd/df
    val historicalData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv")
    val filteredHistoricalData = historicalData.na.drop()


    //create regression object
    val regression = new LinearRegressionWithSGD().setIntercept(true)
    regression.optimizer.setStepSize(0.001)
    regression.optimizer.setNumIterations(500)




    val playersDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(currentData)

    val expandedPlayersDF = playersDF.explode("similarPlayers", "similar"){player: String => player.asInstanceOf[String].split(";")}.drop("similarPlayers").na.drop()

    val filteredExpandedPlayersDF = expandedPlayersDF.filter($"player" !== $"similar")

    filteredExpandedPlayersDF.show()

    val similarPlayerHistory = filteredExpandedPlayersDF.select("player", "similar").join(filteredHistoricalData, filteredExpandedPlayersDF("similar")===filteredHistoricalData("PlayerID"), "left").drop("playerID").na.drop()

    similarPlayerHistory.show()

    //convert age column to double type in data frame
    val toDouble = udf[Double, String]( _.toDouble)
    val similarPlayerHistory2 = similarPlayerHistory.withColumn("Age", toDouble(similarPlayerHistory("age")))

    //convert age column into vector. PolynomialExpansion only works on column
    val assembler = new VectorAssembler()
    .setInputCols(Array("Age"))
    .setOutputCol("age_vector")

    val allSimilarPlayerHistory = assembler.transform(similarPlayerHistory2)

    //choose the degree of polynomial expansion of age vector
    val poly = 3
    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("age_vector")
      .setOutputCol("polyAge")
      .setDegree(poly)

    //add polynomial expansion vector to table
    val polySimilar = polynomialExpansion.transform(allSimilarPlayerHistory)

    //convert back to RDD for ease of use
    val allSimilarPlayers_RDD = polySimilar.rdd

    //group historical data by player
    val grouped_allSimilarPlayers = allSimilarPlayers_RDD.groupBy{
      player => player(0)
    }.collect()

    //vector for data normalization
    //equation for 3rd degree poly would look like (x+ (x^2)/10^2 + (X^3)/10^3 )
    var norm = new Array[Double](poly)
    for (i <- 0 to poly-1){
      if (i == 0) norm(i) = 1
      else norm(i) = math.pow(10,i+1)
    }

    //init file writer
    val pw = new PrintWriter(new File(outputFile))
    pw.println("playerID,intercept,weights")


    //loop through each current player
    for((player, similarPlayers) <- grouped_allSimilarPlayers) {

     //similarPlayers is an iterable, convert to RDD
     val similarPlayers_RDD = sc.parallelize(similarPlayers.toList)

     //
     val labeledPoints = similarPlayers_RDD.map { parts =>

        //Normalize the age vector data
        var array = parts.get(parts.length-1).asInstanceOf[Vector].toArray
        for (i <- 0 to poly-1){
          array(i) = array(i)/norm(i)
        }
        //create labeledPoints RDD
        LabeledPoint(parts.getString(11).toDouble, Vectors.dense(array))

      }.cache()
      //run the regression
      val model = regression.run(labeledPoints)

      //print intercept and weights of model to file for each player
      var weights = ""
      for(i <- 0 to poly-1){
        if (i==poly-1) weights = weights + model.weights(i).toString
        else weights = weights + model.weights(i).toString + ","
      }
      pw.println(player + "," + model.intercept.toString + "," + weights)
    }


    pw.close()
    sc.stop()
  }
}



// Building the model

  //val model = regression.run(parsedClusteredPlayers)

  //println(model.predict(Vectors.dense(20)))

// Evaluate model on training examples and compute training error
  //val valuesAndPreds = parsedData.map { point =>
    //  val prediction = model.predict(point.features)
    //  (point.label, prediction)
  //}
  //val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
  //println("training Mean Squared Error = " + MSE)

// Save and load model
  //model.save(sc, "myModelPath")
  //val sameModel = LinearRegressionModel.load(sc, "myModelPath")
  //println(model.intercept)
