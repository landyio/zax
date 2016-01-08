package io.landy.app.driver

import akka.pattern.pipe
import io.landy.app.actors.ExecutingActor
import io.landy.app.instance._
import io.landy.app.driver.SparkDriverActor.Commands.{TrainClassifierResponse, TrainRegressorResponse}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.HashSet
import scala.collection.{BitSet, GenTraversableOnce}
import scala.concurrent.Future
import scala.language.{postfixOps, higherKinds, reflectiveCalls}

class SparkDriverActor(private val sc: SparkContext) extends ExecutingActor {

  import SparkDriverActor._

  def convert(sample: Seq[(Seq[Double], Double)]): (RDD[LabeledPoint], Int) = {
    val size = sample.head._1.size

    val rdds = sc.parallelize(
        sample.map {
          case (fs, l) =>
            // Sample coming conversion stage must be properly
            // re-constructed & aligned
            assert(fs.size == size)

            new LabeledPoint(label = l, features = new DenseVector(fs.toArray))
        }
      )

    (rdds, size)
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

  private def infer(sample: RDD[LabeledPoint], featuresCount: Int): IndexedSeq[Set[Double]] = {
    val bins = Array((for { i <- 0 until featuresCount } yield HashSet[Double]()) :_*)

    sample.aggregate(bins)(
      //
      // TODO(kudinkin): breaks Spark's serialization
      //
      // (bs, p) => merge(bs,  bs.keys.map { case i => i -> HashSet(p.features(i)) }.toMap)(HashSet.canBuildFrom),
      // (l, r)  => merge(l,   r)                                                          (HashSet.canBuildFrom)

      (bs, p)   => bs.zip(p.features.toArray ).map { case (vs, v) => vs + v },
      (bs, bs0) => bs.zip(bs0).map { case (vs, vs0) => vs.union(vs0) }
    )
  }

  private def remap(ps: RDD[LabeledPoint], supports: IndexedSeq[Set[Double]], categorical: BitSet)
    : (Map[Int, Map[Double, Int]], Set[Int], RDD[LabeledPoint]) = {

    // Detect which features are _explanatory_ (ie particularly relevant ones)
    val explanatory = supports.indices.filter { i => supports(i).size > 1 }.toSet

    // Create mapping replacing raw values (seen in training set for
    // particular _categorical_ feature) with int from range `0 .. support-size`
    val mapping =
      categorical
        .toSeq
        .filter { explanatory(_) }
        .map    { case i => i -> supports(i).zipWithIndex.toMap }
        .toMap

    val remapped =
      ps.map {
        p =>  LabeledPoint(
                p.label,
                Vectors.dense(
                  p.features.toArray
                            .zipWithIndex
                            .map {
                              case (v, i) =>
                                if (mapping.contains(i)) mapping(i)(v).toDouble else v
                            }
                )
              )
      }

    (mapping, explanatory, remapped)
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
    val (converted, featuresCount) = convert(sample)

    // Extract proper categories out of training &
    // build remapping tables for those features
    val supports = infer(converted, featuresCount)

    // Remap actual feature values into 'densified'
    // categorical space
    val (mapping, explanatory, remapped) = remap(converted, supports, categorical)

    (mapping, explanatory, remapped)
  }

  private def trainClassifier(sample: Seq[(Seq[Double], Double)], categorical: BitSet): Future[TrainClassifierResponse] = {
    // TODO(kudinkin): Extract

    val maxBins     = 512
    val maxDepth    = 16
    val impurity    = "gini"
    val numClasses  = 2

    val (mapping, explanatory, remapped)  = prepare(sample, categorical)
    val (train, test)                     = split(remapped)

    val model = DecisionTree.trainClassifier(train, numClasses, squash(mapping), impurity, maxDepth, maxBins)

    val error = evaluate(model, test)

    Future {
      Commands.TrainClassifierResponse(
        new SparkDecisionTreeClassificationModel(model, SparkModel.Extractor(mapping, explanatory)),
        error
      )
    }
  }

  def trainRegressor(sample: Seq[(Seq[Double], Double)], categorical: BitSet): Future[TrainRegressorResponse] = {

    // TODO(kudinkin): Extract

    log.info("Training regressor with sample total of {} elements", sample.size)

    val maxBins     = 512
    val maxDepth    = 24
    val impurity    = "variance"

    val (mapping, explanatory, remapped)  = prepare(sample, categorical)
    val (train, test)                     = split(remapped)

    val model = DecisionTree.trainRegressor(train, squash(mapping), impurity, maxDepth, maxBins)

    val error = evaluate(model, test)

    log.info(s"Finished training regressor {}   \n" +
             s"Sample #:    ${sample.size}      \n" +
             s"Mapping:     {}                  \n" +
             s"Explanatory: {}                  \n" +
             s"Error:       {}                  \n", model, mapping, explanatory, error)

    Future {
      TrainRegressorResponse(
        new SparkDecisionTreeRegressionModel(model, SparkModel.Extractor(mapping, explanatory)),
        error
      )
    }
  }

  override def receive: Receive = trace {
    case Commands.TrainClassifier(sample, categories) =>
      trainClassifier(sample, categories) pipeTo sender()

    case Commands.TrainRegressor(sample, categories)  =>
      trainRegressor(sample, categories) pipeTo sender()
  }

}

object SparkDriverActor {

  object Commands {

    /**
      * Request for training classifier on particular sample
      *
      * @param sample   sample to train classifier on
      */
    case class TrainClassifier(sample: Seq[(Seq[Double], Double)], categories: BitSet) {
      override def toString: String = s"TrainClassifier(Array[${sample.size}], ${categories})"
    }

    case class TrainClassifierResponse(model: ClassificationModel, error: Double)

    /**
      * Request for training regressor on particular sample
      *
      * @param sample   sample to train regressor on
      */
    case class TrainRegressor(sample: Seq[(Seq[Double], Double)], categories: BitSet) {
      override def toString: String = s"TrainRegressor(Array[${sample.size}, ${categories})"
    }

    case class TrainRegressorResponse(model: RegressionModel, error: Double)

  }

}