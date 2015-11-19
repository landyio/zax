package com.flp.control.spark

import akka.pattern.pipe
import com.flp.control.akka.ExecutingActor
import com.flp.control.instance._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

import scala.concurrent.Future

class SparkDriverActor(private val sc: SparkContext) extends ExecutingActor {

  import SparkDriverActor._

  def convert(sample: Seq[(Seq[Double], Double)]): RDD[LabeledPoint] = {
    sc.parallelize(
      sample.map {
        case (fs, l) => new LabeledPoint(label = l, features = new DenseVector(fs.toArray))
      }
    )
  }

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

  private def trainClassifier(sample: Seq[(Seq[Double], Double)]): Future[(ClassificationModel, Double)] = {

    // TODO(kudinkin): Extract

    val maxBins     = 32
    val maxDepth    = 10
    val impurity    = "gini"
    val numClasses  = 2

    val catFeaturesInfo = Map[Int, Int]()

    val (train, test) = split(convert(sample))

    val model = DecisionTree.trainClassifier(train, numClasses, catFeaturesInfo, impurity, maxDepth, maxBins)

    val error = evaluate(model, test)

    // TODO(kudinkin): that's due to pickling being unable generate unpickler for structural-type
    // Future { (new SparkClassificationModel(model), error) }

    Future { (new SparkDecisionTreeClassificationModel(model), error) }
  }


  def trainRegressor(sample: Seq[(Seq[Double], Double)]): Future[(RegressionModel, Double)] = {

    // TODO(kudinkin): Extract

    log.info(s"Training regressor with sample total of ${sample.size} elements")

    val maxBins     = 32
    val maxDepth    = 10
    val impurity    = "variance"

    val catFeaturesInfo = Map[Int, Int]()

    val (train, test) = split(convert(sample))

    val model = DecisionTree.trainRegressor(train, catFeaturesInfo, impurity, maxDepth, maxBins)

    val error = evaluate(model, test)

    log.info( s"Finished training regressor (${model.getClass.getName});\n" +
              s"Error: ${error}\n")

    // TODO(kudinkin): that's due to pickling being unable generate unpickler for structural-type
    // Future { (new SparkRegressionModel(model), error) }

    Future { (new SparkDecisionTreeRegressionModel(model), error) }
  }

  override def receive: Receive = trace {
    case Commands.TrainClassifier(sample) => trainClassifier(sample) pipeTo sender()
    case Commands.TrainRegressor(sample)  => trainRegressor(sample) pipeTo sender()
  }

}

object SparkDriverActor {

  object Commands {

    /**
      * Request for training classifier on particular sample
      *
      * @param sample   sample to train classifier on
      */
    case class TrainClassifier(sample: Seq[(Seq[Double], Double)])
    case class TrainClassifierResponse(model: ClassificationModel, error: Double)

    /**
      * Request for training regressor on particular sample
      *
      * @param sample   sample to train regressor on
      */
    case class TrainRegressor(sample: Seq[(Seq[Double], Double)])
    case class TrainRegressorResponse(model: RegressionModel, error: Double)

  }

}