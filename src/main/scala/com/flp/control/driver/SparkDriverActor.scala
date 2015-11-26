package com.flp.control.driver

import akka.pattern.pipe
import com.flp.control.actors.ExecutingActor
import com.flp.control.instance._
import com.flp.control.driver.SparkDriverActor.Commands.{TrainClassifierResponse, TrainRegressorResponse}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.HashSet
import scala.collection.{BitSet, GenTraversableOnce}
import scala.concurrent.Future
import scala.language.{higherKinds, reflectiveCalls}

class SparkDriverActor(private val sc: SparkContext) extends ExecutingActor {

  import SparkDriverActor._

  def convert(sample: Seq[(Seq[Double], Double)]): RDD[LabeledPoint] = {
    sc.parallelize(
      sample.map {
        case (fs, l) => new LabeledPoint(label = l, features = new DenseVector(fs.toArray))
      }
    )
  }

  def evaluate(model: SparkModel.Model, testSet: RDD[LabeledPoint]): Double = {
    testSet .map    { p => (p.label, model.predict(p.features).round.toDouble) }
            .filter { p => p._1 != p._2 }
            .count
            .toDouble / testSet.count()
  }

  private val TRAIN_TEST_SET_RATIO = 0.8

  private def merge[K, V, S[X] <: Set[X]](one: Map[K, S[V]], other: Map[K, S[V]])(implicit cbf: CanBuildFrom[Nothing, V, S[V]]): Map[K, S[V]] =
    one.foldLeft(other) {
      case (acc, (k, vs)) => acc + (k -> acc(k).union(vs).to[S])
    }

  def split[T <: RDD[_]](dataSet: T) =
    dataSet.randomSplit(Array(TRAIN_TEST_SET_RATIO, 1 - TRAIN_TEST_SET_RATIO)) match {
      case Array(a: T, b: T) => (a, b)
    }

  private def squash[K, V](m: Map[K, GenTraversableOnce[V]]): Map[K, Int] =
    m.map { case (k, vs) => k -> vs.size }

  private def infer(sample: RDD[LabeledPoint], categories: BitSet): Map[Int, Set[Double]] = {
    val bins = Map(categories.toSeq.map { (_, HashSet[Double]()) }: _*)

    sample.aggregate(bins)(
      //
      // TODO(kudinkin): breaks Spark's serialization
      //
      // (bs, p) => merge(bs,  bs.keys.map { case i => i -> HashSet(p.features(i)) }.toMap)(HashSet.canBuildFrom),
      // (l, r)  => merge(l,   r)                                                          (HashSet.canBuildFrom)

      (bs, p) => bs.foldLeft(bs.keys.map { case i => i -> HashSet(p.features(i)) }.toMap) {
        case (acc, (k, vs)) => acc + (k -> acc(k).union(vs))
      },
      (l, r)  => l.foldLeft(r) {
        case (acc, (k, vs)) => acc + (k -> acc(k).union(vs))
      }

    )
  }

  private def remap(ps: RDD[LabeledPoint], categories: Map[Int, Set[Double]]): (Map[Int, Map[Double, Int]], RDD[LabeledPoint]) = {
    val mapping =
      categories.map {
        case (i, vs) => i -> vs.zipWithIndex.toMap
      }

    val remapped =
      ps.map {
        p => LabeledPoint(
          p.label,
          Vectors.dense(
            p.features.toArray
                      .zipWithIndex
                      .map { case (v, i) => mapping(i)(v).toDouble }
          )
        )
      }

    (mapping, remapped)
  }

  /**
    * Prepares sample prior to training to extract info about categories of values
    * and remap every sample point into {0..N} space where N -- is the amount of
    * values in category for a particlar feature
    *
    * @param sample sample to be converted
    * @param categorical bit-set demarking categorical features
    * @return categories-info, mapping from actual values into {0..N} range (see above), and
    *         remapped sample itself
    */
  def prepare(sample: Seq[(Seq[Double], Double)], categorical: BitSet) = {
    // Convert `Seq` into `RDD`
    val converted = convert(sample)

    // Extract proper categories out of training &
    // build remapping tables for those features
    val categories = infer(converted, categorical)

    // Remap actual feature values into 'densified'
    // categorical space
    val (mapping, remapped) = remap(converted, categories)

    (categories, mapping, remapped)
  }

  private def trainClassifier(sample: Seq[(Seq[Double], Double)], categorical: BitSet): Future[TrainClassifierResponse] = {

    // TODO(kudinkin): Extract

    val maxBins     = 32
    val maxDepth    = 10
    val impurity    = "gini"
    val numClasses  = 2

    val (categories, mapping, remapped) = prepare(sample, categorical)
    val (train, test)                   = split(remapped)

    val model = DecisionTree.trainClassifier(train, numClasses, squash(categories), impurity, maxDepth, maxBins)

    val error = evaluate(model, test)

    Future { Commands.TrainClassifierResponse(new SparkDecisionTreeClassificationModel(model, mapping), error) }
  }

  def trainRegressor(sample: Seq[(Seq[Double], Double)], categorical: BitSet): Future[TrainRegressorResponse] = {

    // TODO(kudinkin): Extract

    log.info(s"Training regressor with sample total of ${sample.size} elements")

    val maxBins     = 32
    val maxDepth    = 16
    val impurity    = "variance"

    val (categories, mapping, remapped) = prepare(sample, categorical)
    val (train, test)                   = split(remapped)

    val model = DecisionTree.trainRegressor(train, squash(categories), impurity, maxDepth, maxBins)

    val error = evaluate(model, test)

    log.info( s"Finished training regressor {${model}} \n" +
              s"Sample #:   ${sample.size}             \n" +
              s"Categories: ${categories}              \n" +
              s"Error:      ${error}                   \n")

    Future { TrainRegressorResponse(new SparkDecisionTreeRegressionModel(model, mapping), error) }
  }

  override def receive: Receive = trace {
    case Commands.TrainClassifier(sample, categories) => trainClassifier(sample, categories) pipeTo sender()
    case Commands.TrainRegressor(sample, categories)  => trainRegressor(sample, categories) pipeTo sender()
  }

}

object SparkDriverActor {

  object Commands {

    /**
      * Request for training classifier on particular sample
      *
      * @param sample   sample to train classifier on
      */
    case class TrainClassifier(sample: Seq[(Seq[Double], Double)], categories: BitSet)
    case class TrainClassifierResponse(model: ClassificationModel, error: Double)

    /**
      * Request for training regressor on particular sample
      *
      * @param sample   sample to train regressor on
      */
    case class TrainRegressor(sample: Seq[(Seq[Double], Double)], categories: BitSet)
    case class TrainRegressorResponse(model: RegressionModel, error: Double)

  }

}