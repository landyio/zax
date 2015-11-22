package com.flp.control.instance

import com.flp.control.model.{UserDataDescriptor, UserIdentity, Variation}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

import scala.language.reflectiveCalls
import scala.pickling.directSubclasses
import scala.util.Random


trait Predictor {

  val config: AppInstanceConfig

  def variations:           Seq[Variation]          = config.variations
  def userDataDescriptors:  Seq[UserDataDescriptor] = config.userDataDescriptors

  /**
    * Predicts most relevant variation for the user with supplied identity
    *
    * @param identity user's identity
    * @return         (presumably) most relevant variation
    */
  def predictFor(identity: UserIdentity): Variation

}

object Predictor {
  def apply(config: AppInstanceConfig): Predictor = {
    config.model match {
      case Some(Left(m))  => buildClassifier(config, m)
      case Some(Right(m)) => buildRegressor(config, m)
      case None           => buildRandom(config)
    }
  }

  def random(config: AppInstanceConfig): Predictor =
    buildRandom(config)

  /**
    * Opportunistic predictor, picking variations randomly
    *
    * @return predictor
    **/
  private def buildRandom(config: AppInstanceConfig) =
    Regressor(config)

  /**
    * Predictor backed by classifier
    *
    * @return predictor
    */
  private def buildClassifier(config: AppInstanceConfig, model: ClassificationModel): Predictor =
    Classificator(config, model)

  /**
    * Predictor backed by regressor
    *
    * @return predictor
    */
  private def buildRegressor(config: AppInstanceConfig, model: RegressionModel): Predictor =
    Regressor(config, model)
}

/**
  * Predictor built-up on classification model
  */
trait Classificator extends Predictor {

  val model: ClassificationModel

  def predictFor(identity: UserIdentity): Variation =
    variations(
      model.predict(identity.toFeatures(userDataDescriptors))
    )

}

/**
  * Predictor built-up on regression model
  */
trait Regressor extends Predictor {

  val model: RegressionModel

  override def predictFor(identity: UserIdentity): Variation = {
    val factor: Double = 1e-2
    def rand(): Double = factor * Random.nextInt() / Int.MaxValue

    variations
      .zipWithIndex
      .par // in-parallel
      .toStream
      .map { e => probability(identity, e) -> e }
      .map { case (p, (v, _)) => (p + rand()) -> v }
      .sortBy { case (p, _) => -p }
      .collectFirst { case (_, v) => v }
      .getOrElse(Variation.sentinel)
  }

  private def probability(uid: UserIdentity, variation: (Variation, Int)): Double =
    variation match {
      case (_, idx) =>
        model.predict(uid.toFeatures(userDataDescriptors) ++ Seq(idx.toDouble))
    }


}

/**
  * Classificators
  */
object Classificator {
  def apply(config: AppInstanceConfig, model: ClassificationModel) =
    new ClassificatorImpl(config, model)
}

private[instance] class ClassificatorImpl(override val config:  AppInstanceConfig,
                                          override val model:   ClassificationModel)
  extends Classificator


/**
  * Regressors
  */
object Regressor {

  def apply(config: AppInstanceConfig) =
    new RegressorStub(config)

  def apply(config: AppInstanceConfig, model: RegressionModel) =
    new RegressorImpl(config, model)
}

private[instance] class RegressorImpl(override val config:  AppInstanceConfig,
                                      override val model:   RegressionModel)
  extends Regressor

private[instance] class RegressorStub(override val config: AppInstanceConfig)
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
  val model: T
}

@directSubclasses(Array(classOf[SparkDecisionTreeRegressionModel], classOf[SparkDecisionTreeClassificationModel]))
sealed trait PickleableModel

object SparkModel {

  //
  // - Hey, Joe, do you love ducks?
  // - Quack-quack!
  //

  type Model = {
    def predict(features: Vector): Double
    def predict(features: RDD[Vector]): RDD[Double]
  }
}

class SparkRegressionModel[+T <: SparkModel.Model](override val model: T) extends RegressionModel
                                                                          with    SparkModel[T] {

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  override def predict(seq: Seq[Double]): Double =
    model.predict(new DenseVector(seq.toArray))

}

final case class SparkDecisionTreeRegressionModel(override val model: DecisionTreeModel)
  extends SparkRegressionModel[DecisionTreeModel](model)
  with    PickleableModel


class SparkClassificationModel[+T <: SparkModel.Model](override val model: T) extends ClassificationModel
                                                                              with    SparkModel[T] {

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  override def predict(seq: Seq[Double]): Int =
    model.predict(new DenseVector(seq.toArray)).toInt

}

final case class SparkDecisionTreeClassificationModel(override val model: DecisionTreeModel)
  extends SparkClassificationModel[DecisionTreeModel](model)
  with    PickleableModel

// predicted labels are +1 or -1 for GBT.
//class SparkRandomForestRegressionModel(override val model: RandomForestModel)
//  extends SparkRegressionModel[RandomForestModel]
//
//class SparkDecisionTreeClassificationModel(override val model: DecisionTreeModel)
//  extends SparkRegressionModel[DecisionTreeModel]

