package com.flp.control.instance

import com.flp.control.model.{UserDataDescriptor, UserIdentity, Variation}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.language.reflectiveCalls


trait Predictor {

  /**
    * User-data descriptors converting its identity into point
    * in high-dimensional feature-space
    */
  val userDataDescriptors: Seq[UserDataDescriptor]

  /**
    * Available variations
    */
  val variations: Seq[Variation]

  /**
    * Predicts most relevant variation for the user with supplied identity
    *
    * @param identity user's identity
    * @return         (presumably) most relevant variation
    */
  def predictFor(identity: UserIdentity): Variation

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
  def apply(variations: Seq[Variation], userdata: Seq[UserDataDescriptor], model: ClassificationModel) =
    new ClassificatorImpl(variations, userdata, model)
}

private[instance] class ClassificatorImpl(override val variations:          Seq[Variation],
                                          override val userDataDescriptors: Seq[UserDataDescriptor],
                                          override val model:               ClassificationModel)
  extends Classificator


/**
  * Regressors
  */
object Regressor {

  def apply(variations: Seq[Variation], userdata: Seq[UserDataDescriptor]) =
    new RegressorStub(variations, userdata)

  def apply(variations: Seq[Variation], userdata: Seq[UserDataDescriptor], model: RegressionModel) =
    new RegressorImpl(variations, userdata, model)
}

private[instance] class RegressorImpl(override val variations:          Seq[Variation],
                                      override val userDataDescriptors: Seq[UserDataDescriptor],
                                      override val model:               RegressionModel)
  extends Regressor

private[instance] class RegressorStub(override val variations:          Seq[Variation],
                                      override val userDataDescriptors: Seq[UserDataDescriptor])
  extends RegressorImpl(variations, userDataDescriptors, RegressionModel.random)


//
// TODO(kudinkin): Purge
//
object RegressionModel {
  val random: RegressionModel = new RegressionModel {
    val r = new Random(0xDEADBABE)
    override def predict(vector: Seq[Double]): Double = r.nextDouble()
  }
}


trait RegressionModel {
  def predict(vector: Seq[Double]): Double
}

trait ClassificationModel {
  def predict(vector: Seq[Double]): Int
}

trait SparkModel[T <: SparkModel.Model] {
  val model: T
}

object SparkModel {

  //
  // - Hey, Joe, do you love ducks?
  // - Quack-quack
  //

  type Model = {
    def predict(features: Vector): Double
    def predict(features: RDD[Vector]): RDD[Double]
  }
}


class SparkRegressionModel[T <: SparkModel.Model](override val model: T)  extends RegressionModel
                                                                          with    SparkModel[T] {

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  override def predict(seq: Seq[Double]): Double =
    model.predict(new DenseVector(seq.toArray))

}

class SparkClassificationModel[T <: SparkModel.Model](override val model: T)  extends ClassificationModel
                                                                              with    SparkModel[T] {

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  override def predict(seq: Seq[Double]): Int =
    model.predict(new DenseVector(seq.toArray)).toInt
}

// predicted labels are +1 or -1 for GBT.
//class SparkRandomForestRegressionModel(override val model: RandomForestModel)
//  extends SparkRegressionModel[RandomForestModel]
//
//class SparkDecisionTreeClassificationModel(override val model: DecisionTreeModel)
//  extends SparkRegressionModel[DecisionTreeModel]

