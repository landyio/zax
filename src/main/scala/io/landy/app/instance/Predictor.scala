package io.landy.app.instance

import io.landy.app.model.{UserDataDescriptor, UserIdentity, Variation}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

import scala.language.reflectiveCalls
import scala.pickling.directSubclasses
import scala.util.Random


trait Predictor {

  import Predictor._

  val variations:   Seq[Variation]
  val descriptors:  Seq[UserDataDescriptor]

  /**
    * Predicts most relevant variation for the user with supplied identity
    *
    * @param identity user's identity
    * @return         (presumably) most relevant variation
    */
  def predictFor(identity: UserIdentity): Outcome

}

object Predictor {

  /**
    * Trait representing (atomic) predictor output
    */
  trait Outcome {
    val variation: Variation
  }

  object Outcome {
    case class Randomized(variation: Variation) extends Outcome
    case class Predicted(variation: Variation) extends Outcome
  }


  def apply(config: Instance.Config): Predictor = {
    config.model match {
      case Some(Left(m))  => buildClassifier(config, m)
      case Some(Right(m)) => buildRegressor(config, m)
      case None           => buildRandom(config)
    }
  }

  def random(config: Instance.Config): Predictor =
    buildRandom(config)

  /**
    * Opportunistic predictor, picking variations randomly
    *
    * @return predictor
    **/
  private def buildRandom(config: Instance.Config) =
    Random(config)

  /**
    * Predictor backed by classifier
    *
    * @return predictor
    */
  private def buildClassifier(config: Instance.Config, model: ClassificationModel): Predictor =
    Classificator(config, model)

  /**
    * Predictor backed by regressor
    *
    * @return predictor
    */
  private def buildRegressor(config: Instance.Config, model: RegressionModel): Predictor =
    Regressor(config, model)
}

/**
  * Predictor built-up on classification model
  */
trait Classificator extends Predictor {

  import Predictor._

  val model: ClassificationModel

  def predictFor(identity: UserIdentity): Outcome =
    model.predict(identity.toFeatures(descriptors)) match {
      case id => Outcome.Predicted(variations(id))
    }

}

/**
  * Predictor built-up on regression model
  */
trait Regressor extends Predictor {

  import Predictor._

  val model: RegressionModel

  override def predictFor(identity: UserIdentity): Outcome = {
    val factor: Double = 1e-2
    def rand(): Double = factor * scala.util.Random.nextInt() / Int.MaxValue

    variations
      .zipWithIndex
      .par
      .toStream
      .map { case (v, id) => probabilityFor(identity, id) -> v }
      .map { case (p, v)  => (p + rand()) -> v }
      .sortBy { case (p, _) => -p }
      .collectFirst { case (_, v) => Outcome.Predicted(v) }
      .get
  }

  private def probabilityFor(uid: UserIdentity, varId: Int): Double =
    model.predict(uid.toFeatures(descriptors) ++ Seq(varId.toDouble))

}

/**
  * Classificators
  */
object Classificator {
  def apply(config: Instance.Config, model: ClassificationModel) =
    new ClassificatorImpl(config.variations, config.userDataDescriptors, model)
}

private[instance] class ClassificatorImpl(override val variations:  Seq[Variation],
                                          override val descriptors: Seq[UserDataDescriptor],
                                          override val model:       ClassificationModel)
  extends Classificator


/**
  * Regressors
  */
object Regressor {
  def apply(config: Instance.Config, model: RegressionModel) =
    new RegressorImpl(config.variations, config.userDataDescriptors, model)
}

private[instance] class RegressorImpl(override val variations:  Seq[Variation],
                                      override val descriptors: Seq[UserDataDescriptor],
                                      override val model:   RegressionModel)
  extends Regressor


/**
  * Randomized 'predictor' (guessing variation at random)
  */
object Random {

  def apply(c: Instance.Config) =
    new Predictor {
      override val variations:  Seq[Variation]          = c.variations
      override val descriptors: Seq[UserDataDescriptor] = c.userDataDescriptors

      val r = new Random(0xDEADBABE)

      override def predictFor(identity: UserIdentity): Predictor.Outcome =
        Predictor.Outcome.Randomized(c.variations(r.nextInt(c.variations.length)))
    }
}

sealed trait RegressionModel {
  def predict(vector: Seq[Double]): Double
}

sealed trait ClassificationModel {
  def predict(vector: Seq[Double]): Int
}

/**
  * Abstract feature-extractor trait, allowing to
  * convert features from one Euclidian space to any other Euclidian-space
  */
trait FeatureExtractor {
  def apply(seq: Seq[Double]): Seq[Double]
}


/**
  * Interface abstracing Spark-specific models
  *
  * @tparam T peculiar type of the Spark's model
  */
sealed trait SparkModel[+T <: SparkModel.Model] {
  protected val model: T

  protected val x: FeatureExtractor

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  protected def predictFor(seq: Seq[Double]): Double =
    model.predict(
      Vectors.dense(
        x(seq).toArray
      )
    )
}

object SparkModel {

  /**
    * Mapping from categorical values (squashed to doubles)
    * into {0..N} range (enforced by 'mllib')
    */
  type Mapping = Map[Int, Map[Double, Int]]

  /**
    * - Hey, Joe, do you love ducks?
    * - Quack-quack!
    */
  type Model = {
    def predict(features: Vector): Double
    def predict(features: RDD[Vector]): RDD[Double]
  }

  case class Extractor(
    /**
      * For details see @SparkModel.Mapping
      */
    mapping: SparkModel.Mapping,

    /**
      * Set if indices of features being recognized as
      * an explanatory ones
      */
    explanatory: Set[Int]
  )
    extends FeatureExtractor {

    override def apply(seq: Seq[Double]) =
      seq .zipWithIndex
          .map {
            case (v, i) => if (mapping.contains(i)) mapping(i)(v).toDouble else v
          }
          .toArray
  }

}

@directSubclasses(Array(classOf[SparkDecisionTreeRegressionModel], classOf[SparkDecisionTreeClassificationModel]))
sealed trait PickleableModel

class SparkRegressionModel[+T <: SparkModel.Model](
  override val model: T,
  override val x:     SparkModel.Extractor
) extends RegressionModel
  with    SparkModel[T] {

  override def predict(seq: Seq[Double]): Double = predictFor(seq)

}

final case class SparkDecisionTreeRegressionModel(
  override val model: DecisionTreeModel,
  override val x:     SparkModel.Extractor
) extends SparkRegressionModel[DecisionTreeModel](model, x)
  with    PickleableModel


class SparkClassificationModel[+T <: SparkModel.Model](
  override val model: T,
  override val x:     SparkModel.Extractor
) extends ClassificationModel
  with    SparkModel[T] {

  override def predict(seq: Seq[Double]): Int = predictFor(seq).toInt

}

final case class SparkDecisionTreeClassificationModel(
  override val model: DecisionTreeModel,
  override val x:     SparkModel.Extractor
) extends SparkClassificationModel[DecisionTreeModel](model, x)
  with    PickleableModel

