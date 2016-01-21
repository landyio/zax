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
import scala.language.{postfixOps, higherKinds, reflectiveCalls}

class SparkDriverActor(private val sc: SparkContext) extends ExecutingActor {

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

  def evaluate(model: SparkModel.Model, testSet: RDD[LabeledPoint]): (Double, Double, Double) = {
    val ps = testSet.map { p => (p.label, model.predict(p.features).round.toDouble) }
                    .collect()


    val tp = ps.count { case (a, b) => a == 1 && b == 1 }
    val fp = ps.count { case (a, b) => a == 0 && b == 1 }
    val fn = ps.count { case (a, b) => a == 1 && b == 0 }

    val precision = tp.toDouble / (tp + fp)
    val recall    = tp.toDouble / (tp + fn)

    val error     = (fp + fn).toDouble / testSet.count()

    (error, precision, recall)
  }

  private val SPLIT_RATIO = 0.7

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

  private def infer(sample: RDD[LabeledPoint], featuresCount: Int): IndexedSeq[Set[Double]] = {
    val bins = Array((for { i <- 0 until featuresCount } yield HashSet[Double]()) :_*)

    sample.aggregate(bins)(
      //
      // TODO(kudinkin): breaks Spark's serialization
      //
      // (bs, p) => merge(bs,  bs.keys.map { case i => i -> HashSet(p.features(i)) }.toMap)(HashSet.canBuildFrom),
      // (l, r)  => merge(l,   r)                                                          (HashSet.canBuildFrom)

      (bs, p) => bs.zip(p.features.toArray).map { case (vs, v) => vs + v },
      (l, r)  => l.zip(r).map { case (l, r) => l.union(r) }
    )
  }

  private def remap(ps: RDD[LabeledPoint], supports: IndexedSeq[Set[Double]], categorical: BitSet)
    : (Map[Int, Map[Double, Int]], Set[Int], RDD[LabeledPoint]) = {

    // Detect which features are _explanatory_ (ie particularly relevant ones)
    val explanatory = deviseExplanatory(supports)

    // Create mapping replacing raw values (seen in training set for
    // particular _categorical_ feature) with int from range `0 .. support-size`
    val mapping =
      categorical
        .toSeq
        .filter { explanatory }
        .map    { i => i -> supports(i).zipWithIndex.toMap }
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
    * Utility to devise whether particular feature is explanatory or not
    * (ie of any particular value for the training proc)
    *
    * @param supports features' supports
    * @return         index-set of those decided to be explanatory ones
    */
  def deviseExplanatory(supports: IndexedSeq[Set[Double]]): Set[Int] = {
    supports.indices.filter {
      i => supports(i).size > 1 // && supports(i).size <= 10
    }.toSet
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
    val (converted, featuresCount) = convert(sample)

    // Extract proper categories out of training &
    // build remapping tables for those features
    val supports = infer(converted, featuresCount)

    // Remap actual feature values into 'densified'
    // categorical space
    val (mapping, explanatory, remapped) = remap(converted, supports, categorical)

    (mapping, explanatory, remapped)
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
    * See @RandomForest.trainRegressor for details
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
  ): (T, SparkModel.Mapping, Set[Int], Double) = {

    val (mapping, explanatory, remapped)  = prepare(sample, categorical)
    val (training, test)                  = split(remapped)

    val model = fitter(training, squash(mapping))

    val (error, prec, rec) = evaluate(model, test)

    (model, mapping, explanatory, error)
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
  ): (T, SparkModel.Mapping, Set[Int], Double) = {

    log.info("Training regressor with sample total of {} elements", sample.size)

    val (mapping, explanatory, remapped)  = prepare(sample, categorical)
    val (training, test)                  = split(remapped)

    val model = fitter(training, squash(mapping))

    val (error, prec, rec)  = evaluate(model,     test)
    val (derror, _, _)      = evaluate(Baseline,  test)

    log.info(s"Finished training regressor {}   \n" +
             s"Sample #:    ${sample.size}      \n" +
             s"Mapping:     {}                  \n" +
             s"Explanatory: {}                  \n" +
             s"Trained:                         \n" +
             s"   Error:       $error           \n" +
             s"   Precision:   $prec            \n" +
             s"   Recall:      $rec             \n" +
             s"Dumb:                            \n" +
             s"   Error:       $derror          \n", model, mapping, explanatory)

    (model, mapping, explanatory, error)
  }


  import SparkDriverActor._

  override def receive: Receive = trace {

    // Trains classifier (of particular model-type) given the sample and
    // categorical features info
    case Commands.TrainClassifier(model, sample, categoricalFeatures) =>
      model match {
        case Models.Types.DecisionTree =>
          val (model, mapping, explanatory, error) =
            fitClassifier(sample, categoricalFeatures,
              DecisionTreeClassifierFitter(
                numClasses  = 2,
                maxBins     = 512,
                maxDepth    = 16,
                impurity    = "gini"
              )
            )

          Future {
            Commands.TrainClassifierResponse(
              new SparkDecisionTreeClassificationModel(model, SparkModel.Extractor(mapping, explanatory)),
              error
            )
          } pipeTo sender()

        case Models.Types.RandomForest =>
          val (model, mapping, explanatory, error) =
            fitClassifier(sample, categoricalFeatures,
              RandomForestClassifierFitter(
                numClasses            = 2,
                numTrees              = 3,
                featureSubsetStrategy = "auto",
                maxBins               = 512,
                maxDepth              = 16,
                impurity              = "gini"
              )
            )

          Future {
            Commands.TrainClassifierResponse(
              new SparkRandomForestClassificationModel(model, SparkModel.Extractor(mapping, explanatory)),
              error
            )
          } pipeTo sender()

      }

    // Trains regressor (of particular model-type) given the sample and
    // categorical features info
    case Commands.TrainRegressor(model, sample, categoricalFeatures) =>
      model match {
        case Models.Types.DecisionTree =>
          val (model, mapping, explanatory, error) =
            fitRegressor(sample, categoricalFeatures,
              DecisionTreeRegressorFitter(
                maxBins     = 4096,
                maxDepth    = 24,
                impurity    = "variance"
              )
            )

          Future {
            Commands.TrainRegressorResponse(
              new SparkDecisionTreeRegressionModel(model, SparkModel.Extractor(mapping, explanatory)),
              error
            )
          } pipeTo sender()

        case Models.Types.RandomForest =>
          val (model, mapping, explanatory, error) =
            fitRegressor(sample, categoricalFeatures,
              RandomForestRegressorFitter(
                numTrees              = math.sqrt(sample.length).toInt,
                featureSubsetStrategy = "auto",
                maxBins               = 4096,
                maxDepth              = 24,
                impurity              = "variance"
              )
            )

          Future {
            Commands.TrainRegressorResponse(
              new SparkRandomForestRegressionModel(model, SparkModel.Extractor(mapping, explanatory)),
              error
            )
          } pipeTo sender()
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
    case class TrainClassifier(m: Models.Types.Type, sample: Seq[(Seq[Double], Double)], categoricalFeatures: BitSet) {
      override def toString: String = s"TrainClassifier(Array[${sample.size}], $categoricalFeatures)"
    }

    case class TrainClassifierResponse(model: ClassificationModel, error: Double)

    /**
      * Request for training regressor on particular sample
      *
      * @param sample sample to train regressor on
      */
    case class TrainRegressor(m: Models.Types.Type, sample: Seq[(Seq[Double], Double)], categoricalFeatures: BitSet) {
      override def toString: String = s"TrainRegressor(Array[${sample.size}, $categoricalFeatures)"
    }

    case class TrainRegressorResponse(model: RegressionModel, error: Double)

  }

}