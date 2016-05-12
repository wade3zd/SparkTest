package com.test

/**
  * Created by zhangdong on 2016/5/3.
  * CREATE TABLE `blog2` (
  `name` varchar(255) NOT NULL,
  `count` double unsigned DEFAULT NULL
) ENGINE=InnoDB
  */
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import com.test.RDDtoMysql.myFun

// $example off$

object LinearRegressionWithSGDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearRegressionWithSGDExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val data=sc.textFile("/root/GDP.txt")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(math.log(parts(0).toDouble), Vectors.dense(parts(1).split(' ').map(a =>math.log(a.toDouble))))
    }.cache()
    val numIterations = 1
    val stepSize = 0.0054
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val d = Array(math.log(2144972),math.log(0.191243543),math.log(0.421668374),math.log(0.094))
    val v = Vectors.dense(d)
    val predictValue=math.exp(model.predict(v))
    println(predictValue)
    val inputData = sc.parallelize(List(("predictValue", predictValue.toDouble)))
    inputData.foreachPartition(myFun)

   /* val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    val sameModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    // $example off$*/

    sc.stop()
  }

}
