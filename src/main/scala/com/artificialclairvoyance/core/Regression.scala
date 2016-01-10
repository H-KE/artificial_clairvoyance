package com.regression.core

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LassoWithSGD
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object Regression {
  def mean[T](item:Traversable[T])(implicit n:Numeric[T]) = {
	n.toDouble(item.sum) / item.size.toDouble
  }

  def main(args: Array[String]) {

  	val conf = new SparkConf().setAppName("Regression")
	val sc = new SparkContext(conf)

	// Load and parse the data
	val battingData = sc.textFile("src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv")
	val homeruns = battingData.map(_.split(",")).filter(line => !line(11).isEmpty).map{ line =>
		(line(22).toDouble, line(11).toDouble)
		}.groupByKey().sortByKey()

	//for ((k,v) <- homeruns) println(k + " -> " + v)

	val maxLen = homeruns.map(_._2.size).max
	val avgHR = homeruns.map(line => (line._1, line._2.toArray, mean(line._2)))
	val parsedBattingData = avgHR.map { line =>
		LabeledPoint(line._1, Vectors.dense(line._2.padTo(maxLen, line._3)))
	}.cache()

	//Building the model
	val numIterations = 100
	val model =  LassoWithSGD.train(parsedBattingData, numIterations)

	// Evaluate model on training examples and compute training error
	val valuesAndPreds = parsedBattingData.map { point =>
	  val prediction = model.predict(point.features)
	  (point.label, prediction)
	}
	val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
	println("training Mean Squared Error = " + MSE)

	// Save and load model
	model.save(sc, "myModelPath")
	val sameModel = LinearRegressionModel.load(sc, "myModelPath")

	sc.stop()
  }
}