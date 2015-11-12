package com.flp.control.spark

import akka.pattern.pipe
import com.flp.control.akka.DefaultActor
import com.flp.control.instance.{SparkRegressionModel, RegressionModel}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

import scala.concurrent.Future

class SparkDriverActor(private val sc: SparkContext) extends DefaultActor {

  import SparkDriver._

  def convert(sample: Seq[Any]): RDD[LabeledPoint] = null

  def evaluate(model: DecisionTreeModel, testSet: RDD[LabeledPoint]): Double =
    testSet .map    { p => (p.label, model.predict(p.features)) }
            .filter { p => p._1 != p._2 }
            .count
            .toDouble / testSet.count()

  private val TRAIN_TEST_SET_RATIO = 0.8

  def split[T <: RDD[_]](dataSet: T) =
    dataSet.randomSplit(Array(TRAIN_TEST_SET_RATIO, 1 - TRAIN_TEST_SET_RATIO)) match {
      case Array(a: T, b: T) => (a, b)
    }

  private def train(sample: Seq[Any]): Future[(RegressionModel, Double)] = {

    // TODO(kudinkin): Extract

    val maxBins     = 32
    val maxDepth    = 10
    val impurity    = "gini"
    val numClasses  = 2

    val catFeaturesInfo = Map[Int, Int]()

    val (train, test) = split(convert(sample))

    val model = DecisionTree.trainClassifier(train, numClasses, catFeaturesInfo, impurity, maxDepth, maxBins)

    val error = evaluate(model, test)

    Future { (new SparkRegressionModel(model), error) }
  }


  override def receive: Receive = trace {
    case Commands.Train(sample) => train(sample) pipeTo sender()
  }

}

object SparkDriver {

  object Commands {

    /**
      * Request for training model on particular sample
      *
      * @param sample   sample to train model on
      */
    case class Train(sample: Seq[Any])

  }

}