package com.artificialclairvoyance.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import ArtificialClairvoyance._

import java.io._
import scala.io._

object Regression {
  def main(args: Array[String]) {

  	val conf = new SparkConf().setAppName("Regression")
    val sc = new SparkContext(conf)

    val outputFile = "src/test/resources/output/mlb_playsers2014_models.csv"
    //Homeruns mapped to age 
    val currentData = "src/test/resources/output/mlb_players2014.csv"


  	val historicalData = sc.textFile("src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv")
  	val filteredHistoricalData = historicalData.map(_.split(',')).filter(line =>  line(11).forall(_.isDigit) && !line(11).isEmpty)


  	val regression = new LinearRegressionWithSGD().setIntercept(true)

	regression.optimizer.setStepSize(0.01)

	regression.optimizer.setNumIterations(100)


	
	val pw = new PrintWriter(new File(outputFile))
	pw.println("playerID, intercept, weight")
	//only want to model 5 players for testing, read 5 lines and stuff in array
	var lines = new Array[String](20)

	Source.fromFile(currentData).getLines().copyToArray(lines)
  	for(line <- lines){
  		val parts = line.split(',')
  		val similarPlayers = parts(5).split(';').toArray
  		val similarPlayerHistory = filteredHistoricalData.filter( player => similarPlayers.contains(player(0)))

  		if(similarPlayerHistory.count != 0){
  			val labeledPoints = similarPlayerHistory.map { parts =>
  				LabeledPoint(parts(11).toDouble, Vectors.dense(parts(22).toDouble))
			}.cache()
			val model = regression.run(labeledPoints)
			pw.println(parts(1) + ' ' + model.intercept.toString + ' ' + model.weights(0).toString)
		}

  	}
  		pw.close()
  	//models.foreach(model => println(model))

  	/*printToFile(new File(outputFile)) {
      p => {
        p.println("playerID, Intercept, Weight ")
        models.foreach(model => p.println("%s,%s"
          .format(model(1), model(2))))
      }
    }*/

      

  	//val similarPlayers = Array("bondsba01","burksel01","mcgrifr01","palmera01","bondsba01","griffke02","ortizda01")

  	//val clusteredPlayers = filteredHistoricalData.filter( player => similairPlayers.contains(player(0)))

	

// Building the model
	
	//val model = regression.run(parsedClusteredPlayers)

	//println(model.predict(Vectors.dense(20)))

// Evaluate model on training examples and compute training error
	//val valuesAndPreds = parsedData.map { point =>
  	//	val prediction = model.predict(point.features)
  	//	(point.label, prediction)
	//}
	//val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
	//println("training Mean Squared Error = " + MSE)

// Save and load model
	//model.save(sc, "myModelPath")
	//val sameModel = LinearRegressionModel.load(sc, "myModelPath")
	//println(model.intercept)

	}	
}




/*
val data = sc.textFile("/Users/kehan/Spark/artificial-clairvoyance/src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv”)

val filteredData = data.map(_.split(",")).filter( line => line(11).forall(_.isDigit) && !line(11).isEmpty)

val similarPlayers = Array("bondsba01","burksel01","mcgrifr01","palmera01","bondsba01","griffke02","ortizda01”)

val clusteredPlayers = filteredData.filter( player => similarPlayers.contains(player(0)))

val parsedClusteredPlayers = clusteredPlayers.map { parts => LabeledPoint(parts(11).toDouble, Vectors.dense(parts(22).toDouble))}.cache()

val regression = new mllib.regression.LinearRegressionWithSGD().setIntercept(true)

regression.optimizer.setStepSize(0.01)

regression.optimizer.setNumIterations(1000)

val model = regression.run(parsedClusteredPlayers)

model.predict(mllib.linalg.Vectors.dense(20))
*/