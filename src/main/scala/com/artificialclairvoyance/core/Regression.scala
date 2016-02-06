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
import org.apache.spark.mllib.linalg.Vectors
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

    val polynomialExpansion = new PolynomialExpansion()
	.setInputCol("age_vector")
	.setOutputCol("polyAge")
	.setDegree(3)

    val historicalData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv")
    val filteredHistoricalData = historicalData.na.drop()

  	// val historicalData = sc.textFile("src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv")
  	// val filteredHistoricalData = historicalData.map(_.split(','))
  	// 								.filter(line =>  line(11).forall(_.isDigit) && !line(11).isEmpty)
  	// 								.map( p => 
			// 						MLB_Batting(
			// 							p(0), p(1), p(2),
			// 							p(3), p(4), p(5), p(6),
			// 							p(7), p(8), p(9),
			// 							p(10), p(11), p(12),
			// 							p(13), p(14), p(15),
			// 							p(16), p(17), p(18),
			// 							p(19), p(20), p(22))
			// 						).toDF()

	// registerTempTable() creates an in-memory table that is scoped to the cluster in which it was created
	// saveAsTable() creates a permanent, physical table stored in S3 using the Parquet format.
	//filteredHistoricalData.registerTempTable("HistoricalData")

  	val regression = new LinearRegressionWithSGD().setIntercept(true)
	regression.optimizer.setStepSize(0.01)
	regression.optimizer.setNumIterations(100)


	val pw = new PrintWriter(new File(outputFile))
	pw.println("playerID,intercept,weight")
	//only want to model 5 players for testing, read 5 lines and stuff in array
	var lines = new Array[String](20)

	val playersDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(currentData)

	// val playersDF = sc.textFile(currentData)
	// 						.map(_.split(','))
	// 						.filter(line =>  line(0).forall(_.isDigit))
	// 						.map( p => Players(	p(0), p(1), p(2),
	// 											p(3), p(4), p(5) ) )
	// 						.toDF()
	val expandedPlayersDF = playersDF.explode("similarPlayers", "similar"){player: String => player.asInstanceOf[String].split(";")}.drop("similarPlayers").na.drop()

	val filteredExpandedPlayersDF = expandedPlayersDF.filter($"player" !== $"similar")

	filteredExpandedPlayersDF.show()

	//expandedPlayersDF.registerTempTable("Players")

	//val groupedSimilarPlayersDF = playersDF.groupBy("player")

	//val historicalPlayersSQL = sqlContext.sql("SELECT * FROM HistoricalData")

	//val similarPlayersSQL = sqlContext.sql("SELECT Players.player, Players.similar FROM Players")

	//val similarPlayers = similarPlayersSQL.map(player => player).collect().foreach(println)

	//val similarPlayerHistorySQL = sqlContext.sql("SELECT Players.player, HistoricalData.* FROM HistoricalData LEFT JOIN Players ON HistoricalData.playerID=Players.similar")

	val similarPlayerHistory = filteredExpandedPlayersDF.select("player", "similar").join(filteredHistoricalData, filteredExpandedPlayersDF("similar")===filteredHistoricalData("playerID"), "left").drop("playerID").na.drop()
  
  //convert age column to double type in data frame
  val toDouble = udf[Double, String]( _.toDouble)

  val similarPlayerHistory2 = similarPlayerHistory.withColumn("Age", toDouble(similarPlayerHistory("age")))

  val assembler = new VectorAssembler()
  .setInputCols(Array("Age"))
  .setOutputCol("age_vector")

  
  val allSimilarPlayerHistory = assembler.transform(similarPlayerHistory2)

	//val similarPlayerHistory = similarPlayerHistorySQL.collect()
	//similarPlayerHistory.foreach(println)

	val players = playersDF.select("player")
	val player = players.first()

	val polySimilar = polynomialExpansion.transform(allSimilarPlayerHistory)


  //IT WORKS HERE
  val similarPlayers = polySimilar.filter($"player" === player(0))

  similarPlayers.show()

  polySimilar.show()


  /*******************
  filtering throws exception while looping
  ****************/

	for (player <- players) {
    try{
			println(player(0))
      val similarPlayers = polySimilar.filter("player = " + player(0))
      println(similarPlayers.count)
      //print(similarPlayers.getDouble(11))

			 //val labeledPoints = similarPlayers.map { similar =>
       //                   println(similar)
			 										//LabeledPoint(similar.getDouble(11), Vectors.dense(similar.get(26)))
			       							
      // println(labeledPoints.count)
			// println(labeledPoints.count)

			// val model = regression.run(labeledPoints)

			//println(model.intercept.toString + ',' + model.weights(0).toString)
		}catch{
      case e: Exception => 
        //println("error")
    }
	}

	//pw.close()

	sc.stop()

	// if(similarPlayerHistory.count != 0){
	// 	val model = similarPlayerHistory.map { player =>
	// 						val polyAge = polynomialExpansion.transform(player.getAs[Double]("age"))
	// 						labeledPoint = LabeledPoint(player(11).toDouble, Vectors.dense(polyAge))
	// 						val prediction = regression.run(labeledPoints)
	// 						(player(1), prediction.intercept.toString, model.weights(0).toString)

	// 	}.cache()
	// 	model.saveAsTextFile(outputFile)
	// }

	// Source.fromFile(currentData).getLines().copyToArray(lines)
 //  	for(line <- lines){
 //  		val parts = line.split(',')
 //  		val similarPlayers = parts(5).split(';').toArray
 //  		val similarPlayerHistory = filteredHistoricalData.filter( player => similarPlayers.contains(player(0)))

 //  		if(similarPlayerHistory.count != 0){
 //  			val labeledPoints = similarPlayerHistory.map { parts =>
 //  				//val df = sqlContext.createDataFrame(parts(22).toDouble).toDF("age")
 //  				//val polyAge = polynomialExpansion.transform(parts(22))
 //  				LabeledPoint(parts(11).toDouble, Vectors.dense(polyAge))
	// 		}.cache()
	// 		val model = regression.run(labeledPoints)
	// 		pw.println(parts(1) + ',' + model.intercept.toString + ',' + model.weights(0).toString)
	// 	}

 //  	}
  		//pw.close()
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