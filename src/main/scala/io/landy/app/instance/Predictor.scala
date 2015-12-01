package io.landy.app.instance

import io.landy.app.model.{UserDataDescriptor, UserIdentity, Variation}
import org.apache.spark.mllib.linalg.{Vectors, DenseVector, Vector}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

import scala.language.reflectiveCalls
import scala.pickling.directSubclasses
import scala.util.Random


trait Predictor {

  val config: Instance.Config

  def variations:           Seq[Variation]          = config.variations
  def userDataDescriptors:  Seq[UserDataDescriptor] = config.userDataDescriptors

  /**
    * Predicts most relevant variation for the user with supplied identity
    *
    * @param identity user's identity
    * @return         (presumably) most relevant variation
    */
  def predictFor(identity: UserIdentity): (Variation.Id, Variation)

}

object Predictor {
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
    Regressor(config)

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

  val model: ClassificationModel

  def predictFor(identity: UserIdentity): (Variation.Id, Variation) =
    model.predict(identity.toFeatures(userDataDescriptors)) match {
      case id => (id, variations(id))
    }

}

/**
  * Predictor built-up on regression model
  */
trait Regressor extends Predictor {

  val model: RegressionModel

  override def predictFor(identity: UserIdentity): (Variation.Id, Variation) = {
    val factor: Double = 1e-2
    def rand(): Double = factor * Random.nextInt() / Int.MaxValue

    variations
      .zipWithIndex
      .par
      .toStream
      .map { case (v, id) => probability(identity, id) -> (id, v) }
      .map { case (p, (id, v))  => (p + rand()) -> (id, v) }
      .sortBy { case (p, _) => -p }
      .collectFirst { case (_, (id, v)) => (id, v) }
      .get
  }

  private def probability(uid: UserIdentity, varId: Int): Double =
    model.predict(uid.toFeatures(userDataDescriptors) ++ Seq(varId.toDouble))

}

/**
  * Classificators
  */
object Classificator {
  def apply(config: Instance.Config, model: ClassificationModel) =
    new ClassificatorImpl(config, model)
}

private[instance] class ClassificatorImpl(override val config:  Instance.Config,
                                          override val model:   ClassificationModel)
  extends Classificator


/**
  * Regressors
  */
object Regressor {

  def apply(config: Instance.Config) =
    new RegressorStub(config)

  def apply(config: Instance.Config, model: RegressionModel) =
    new RegressorImpl(config, model)
}

private[instance] class RegressorImpl(override val config:  Instance.Config,
                                      override val model:   RegressionModel)
  extends Regressor

private[instance] class RegressorStub(override val config: Instance.Config)
  extends RegressorImpl(config, RegressionModel.random)


//
// TODO(kudinkin): Purge
//
object RegressionModel {
  val random: RegressionModel = new RegressionModel {
    val r = new Random(0xDEADBABE)
    override def predict(vector: Seq[Double]): Double = r.nextDouble()
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

@directSubclasses(Array(classOf[SparkDecisionTreeRegressionModel], classOf[SparkDecisionTreeClassificationModel]))
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

final case class SparkDecisionTreeRegressionModel(override val model: DecisionTreeModel, override val mapping: SparkModel.Mapping)
  extends SparkRegressionModel[DecisionTreeModel](model, mapping)
  with    PickleableModel


class SparkClassificationModel[+T <: SparkModel.Model](
  override val model: T,
  override val mapping: SparkModel.Mapping
) extends ClassificationModel
  with    SparkModel[T] {

  override def predict(seq: Seq[Double]): Int = predictFor(seq).toInt

}

final case class SparkDecisionTreeClassificationModel(
  override val model: DecisionTreeModel,
  override val mapping: SparkModel.Mapping
) extends SparkClassificationModel[DecisionTreeModel](model, mapping)
  with    PickleableModel

