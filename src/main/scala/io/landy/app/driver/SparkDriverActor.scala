package io.landy.app.driver

import akka.pattern.pipe
import io.landy.app.actors.ExecutingActor
import io.landy.app.instance._
import io.landy.app.ml._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, RandomForestModel}
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.rdd.RDD

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.HashSet
import scala.collection.{BitSet, GenTraversableOnce}
import scala.concurrent.Future
import scala.language.{higherKinds, reflectiveCalls}

class SparkDriverActor(private val sc: SparkContext) extends ExecutingActor {

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

  private val SPLIT_RATIO = 0.8

  private def merge[K, V, S[X] <: Set[X]](one: Map[K, S[V]], other: Map[K, S[V]])(implicit cbf: CanBuildFrom[Nothing, V, S[V]]): Map[K, S[V]] =
    one.foldLeft(other) {
      case (acc, (k, vs)) => acc + (k -> acc(k).union(vs).to[S])
    }

  def split[T <: RDD[_]](dataSet: T) =
    dataSet.randomSplit(Array(SPLIT_RATIO, 1 - SPLIT_RATIO)) match {
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
    * values in category for a particular feature
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

  /**
    * Abstract facade incorporating settings to fit model of particular type @T
    *
    * @tparam T type of the model to be fit with
    */
  trait Fitter[+T <: SparkModel.Model] {
    def apply(sample: RDD[LabeledPoint], categoricalFeaturesInfo: Map[Int, Int]): T
  }

  trait ClassifierFitter  [+T <: SparkModel.Model] extends Fitter[T]
  trait RegressorFitter   [+T <: SparkModel.Model] extends Fitter[T]

  /**
    * See @DecisionTree.trainClassifier for details
    */
  case class DecisionTreeClassifierFitter(
    numClasses: Int,
    maxBins:    Int,
    maxDepth:   Int,
    impurity:   String
  ) extends ClassifierFitter[DecisionTreeModel] {

    override def apply(sample: RDD[LabeledPoint], categoricalFeatures: Map[Int, Int]): DecisionTreeModel =
      DecisionTree.trainClassifier(sample, numClasses, categoricalFeatures, impurity, maxDepth, maxBins)

  }

  /**
    * See @DecisionTree.trainClassifier for details
    */
  case class RandomForestClassifierFitter(
    numClasses:             Int,
    numTrees:               Int,
    featureSubsetStrategy:  String,
    impurity:               String,
    maxBins:                Int,
    maxDepth:               Int
  ) extends ClassifierFitter[RandomForestModel] {

    override def apply(sample: RDD[LabeledPoint], categoricalFeatures: Map[Int, Int]): RandomForestModel =
      RandomForest.trainClassifier(sample, numClasses, categoricalFeatures, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

  }

  /**
    * See @DecisionTree.trainRegressor for details
    */
  case class DecisionTreeRegressorFitter(
    maxBins:    Int,
    maxDepth:   Int,
    impurity:   String
  ) extends RegressorFitter[DecisionTreeModel] {

    override def apply(sample: RDD[LabeledPoint], categoricalFeatures: Map[Int, Int]): DecisionTreeModel =
      DecisionTree.trainRegressor(sample, categoricalFeatures, impurity, maxDepth, maxBins)

  }

  /**
    * See @DecisionTree.trainRegressor for details
    */
  case class RandomForestRegressorFitter(
    numTrees:               Int,
    featureSubsetStrategy:  String,
    impurity:               String,
    maxBins:                Int,
    maxDepth:               Int
  ) extends RegressorFitter[RandomForestModel] {

    override def apply(sample: RDD[LabeledPoint], categoricalFeatures: Map[Int, Int]): RandomForestModel =
      RandomForest.trainRegressor(sample, categoricalFeatures, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

  }


  /**
    * Fits classifier of particular type to the sample supplied
    *
    * @param sample       target sample to be fit to (and also cross-validate)
    * @param categorical  set containing indexes of the categorical features
    * @param fitter       particular fitting procedure
    * @tparam T           target model to be fit
    * @return             model fit, mapping of features, cross-validation error
    */
  private def fitClassifier[T <: SparkModel.Model](
    sample: Seq[(Seq[Double], Double)],
    categorical: BitSet,
    fitter: ClassifierFitter[T]
  ): (T, SparkModel.Mapping, Double) = {

    val (categories, mapping, remapped) = prepare(sample, categorical)
    val (training, test)                = split(remapped)

    val model = fitter(training, squash(categories))

    val error = evaluate(model, test)

    (model, mapping, error)
  }


  /**
    * Fits regressor of particular type to the sample supplied
    *
    * @param sample       target sample to be fit to (and also cross-validate)
    * @param categorical  set containing indexes of the categorical features
    * @param fitter       particular fitting procedure
    * @tparam T           target model to be fit
    * @return             model fit, mapping of features, cross-validation error
    */
  def fitRegressor[T <: SparkModel.Model](
    sample: Seq[(Seq[Double], Double)],
    categorical: BitSet,
    fitter: RegressorFitter[T]
  ): (T, SparkModel.Mapping, Double) = {

    log.info("Training regressor with sample total of {} elements", sample.size)

    val (categories, mapping, remapped) = prepare(sample, categorical)
    val (training, test)                = split(remapped)

    val model = fitter(training, squash(categories))

    val error = evaluate(model, test)

    log.info(s"Finished training regressor {} \n" +
             s"Sample #:   {}                 \n" +
             s"Categories: {}                 \n" +
             s"Error:      {}                 \n", model, sample.size, categories, error)

    (model, mapping, error)
  }


  import SparkDriverActor._

  override def receive: Receive = trace {

    // Trains classifier (of particular model-type) given the sample and
    // categorical features info
    case Commands.TrainClassifier(model, sample, categoricalFeatures) =>
      model match {
        case Models.Types.DecisionTree =>
          val (model, mapping, error) =
            fitClassifier(sample, categoricalFeatures,
              DecisionTreeClassifierFitter(
                numClasses  = 2,
                maxBins     = 32,
                maxDepth    = 10,
                impurity    = "gini"
              )
            )

          Future { Commands.TrainClassifierResponse(new SparkDecisionTreeClassificationModel(model, mapping), error) } pipeTo sender()

        case Models.Types.RandomForest =>
          val (model, mapping, error) =
            fitClassifier(sample, categoricalFeatures,
              RandomForestClassifierFitter(
                numClasses            = 2,
                numTrees              = 3,
                featureSubsetStrategy = "auto",
                maxBins               = 32,
                maxDepth              = 10,
                impurity              = "gini"
              )
            )

          Future { Commands.TrainClassifierResponse(new SparkRandomForestClassificationModel(model, mapping), error) } pipeTo sender()

      }

    // Trains regressor (of particular model-type) given the sample and
    // categorical features info
    case Commands.TrainRegressor(model, sample, categoricalFeatures) =>
      model match {
        case Models.Types.DecisionTree =>
          val (model, mapping, error) =
            fitRegressor(sample, categoricalFeatures,
              DecisionTreeRegressorFitter(
                maxBins     = 32,
                maxDepth    = 16,
                impurity    = "variance"
              )
            )

          Future { Commands.TrainRegressorResponse(new SparkDecisionTreeRegressionModel(model, mapping), error) } pipeTo sender()

        case Models.Types.RandomForest =>
          val (model, mapping, error) =
            fitRegressor(sample, categoricalFeatures,
              RandomForestRegressorFitter(
                numTrees              = 3,
                featureSubsetStrategy = "auto",
                maxBins               = 32,
                maxDepth              = 16,
                impurity              = "variance"
              )
            )

          Future { Commands.TrainRegressorResponse(new SparkRandomForestRegressionModel(model, mapping), error) } pipeTo sender()
      }

  }

}

object SparkDriverActor {

  object Commands {

    /**
      * Request for training classifier on particular sample
      *
      * @param sample sample to train classifier on
      */
    case class TrainClassifier(m: Models.Types.Type, sample: Seq[(Seq[Double], Double)], categoricalFeatures: BitSet)
    case class TrainClassifierResponse(model: ClassificationModel, error: Double)

    /**
      * Request for training regressor on particular sample
      *
      * @param sample sample to train regressor on
      */
    case class TrainRegressor(m: Models.Types.Type, sample: Seq[(Seq[Double], Double)], categoricalFeatures: BitSet)
    case class TrainRegressorResponse(model: RegressionModel, error: Double)

  }

}