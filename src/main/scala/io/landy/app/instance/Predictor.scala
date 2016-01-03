package io.landy.app.instance

import io.landy.app.model.{UserDataDescriptor, UserIdentity, Variation}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.model.{RandomForestModel, DecisionTreeModel}
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

sealed trait SparkModel[+T <: SparkModel.Model] {
  protected val model: T
  protected val mapping: SparkModel.Mapping

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  protected def predictFor(seq: Seq[Double]): Double =
    model.predict(
      Vectors.dense(
        seq .zipWithIndex
            .map { case (v, i) => mapping(i)(v).toDouble }
            .toArray
      )
    )
}

@directSubclasses(
  Array(
    classOf[SparkDecisionTreeRegressionModel], classOf[SparkDecisionTreeClassificationModel],
    classOf[SparkRandomForestRegressionModel], classOf[SparkRandomForestClassificationModel]
  )
)
sealed trait PickleableModel

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
}

class SparkRegressionModel[+T <: SparkModel.Model](
  override val model:   T,
  override val mapping: SparkModel.Mapping
) extends RegressionModel
  with    SparkModel[T] {

  override def predict(seq: Seq[Double]): Double = predictFor(seq)

}

/**
  * Spark's MLLib _regression_ decision-tree-model facade
  *
  * @param model    target decision-tree-model
  * @param mapping  see @SparkRegressionModel for details
  */
final case class SparkDecisionTreeRegressionModel(override val model: DecisionTreeModel, override val mapping: SparkModel.Mapping)
  extends SparkRegressionModel[DecisionTreeModel](model, mapping)
  with    PickleableModel

/**
  * Spark's MLLib _regression_ random-forest-model facade
  *
  * @param model    target random-forest-model
  * @param mapping  see @SparkRegressionModel for details
  */
final case class SparkRandomForestRegressionModel(override val model: RandomForestModel, override val mapping: SparkModel.Mapping)
  extends SparkRegressionModel[RandomForestModel](model, mapping)
  with    PickleableModel


class SparkClassificationModel[+T <: SparkModel.Model](
  override val model: T,
  override val mapping: SparkModel.Mapping
) extends ClassificationModel
  with    SparkModel[T] {

  override def predict(seq: Seq[Double]): Int = predictFor(seq).toInt

}

/**
  * Spark's MLLib _classification_ decision-tree-model facade
  *
  * @param model    target decision-tree-model
  * @param mapping  see @SparkRegressionModel for details
  */
final case class SparkDecisionTreeClassificationModel(override val model: DecisionTreeModel, override val mapping: SparkModel.Mapping)
  extends SparkClassificationModel[DecisionTreeModel](model, mapping)
  with    PickleableModel

/**
  * Spark's MLLib _classification_ random-forest-model facade
  *
  * @param model    target random-forest-model
  * @param mapping  see @SparkRegressionModel for details
  */
final case class SparkRandomForestClassificationModel(override val model: RandomForestModel, override val mapping: SparkModel.Mapping)
  extends SparkClassificationModel[RandomForestModel](model, mapping)
  with    PickleableModel

