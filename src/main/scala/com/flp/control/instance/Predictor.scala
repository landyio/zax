package com.flp.control.instance

import com.flp.control.model.{UserIdentity, Variation}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.language.reflectiveCalls


//
// TODO(kudinkin): Kill this madness
//

trait Predictor {

  val variations: Map[Int, Variation] // { variant.index -> variant.value }
  val userDataDescriptors: Seq[UserDataDescriptor] // { request parameter }
  val model: RegressionModel

  def predictFor(identity: UserIdentity): Variation = {
    val factor: Double = 1e-2
    def rand(): Double = factor * Random.nextInt() / Int.MaxValue

    variations
      .par // in-parallel
      .toStream
      .map { e => probability(identity, e) -> e }
      .map { case (p, (k, v)) => (p + rand()) -> v }
      .sortBy { case (p, _) => -p }
      .collectFirst { case (_, v) => v }
      .getOrElse(Variation.sentinel)
  }

  def probability(uid: UserIdentity, variant: (Int, Variation)) =
    variant match {
      case (idx, _) =>
        model.predict(userDataDescriptors.map { d => d.hash(uid.params.get(d.name)) } ++ Seq(idx.toDouble))
    }

}

object Predictor {
  def apply(variants: Map[Int, Variation], userdata: Seq[UserDataDescriptor]) = new PredictorStub(variants, userdata)
}

private[instance] class PredictorImpl(override val variations: Map[Int, Variation],
                                      override val userDataDescriptors: Seq[UserDataDescriptor],
                                      override val model: RegressionModel)
  extends Predictor

private[instance] class PredictorStub(override val variations: Map[Int, Variation],
                                      override val userDataDescriptors: Seq[UserDataDescriptor])
  extends PredictorImpl(variations, userDataDescriptors, RegressionModel.empty)



trait RegressionModel {
  def predict(vector: Seq[Double]): Double
}

// TODO(kudinkin): Purge

object RegressionModel {
  val empty: RegressionModel = new RegressionModel {
    override def predict(vector: Seq[Double]): Double = {
      // System.out.println(Thread.currentThread().getName)
      0d
    }
  }
}

trait SparkModel[T <: SparkModel.Type] {
  val model: T
}

object SparkModel {

  //
  // - Hey, Joe, do you love ducks?
  // - Quack-quack
  //
  type Type = {
    def predict(features: Vector): Double
    def predict(features: RDD[Vector]): RDD[Double]
  }
}


class SparkRegressionModel[T <: SparkModel.Type](override val model: T) extends RegressionModel
                                                                        with    SparkModel[T] {

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  override def predict(vector: Seq[Double]): Double =
    model.predict(new DenseVector(vector.toArray))

}

// predicted labels are +1 or -1 for GBT.
//class SparkRandomForestRegressionModel(override val model: RandomForestModel)
//  extends SparkRegressionModel[RandomForestModel]
//
//class SparkDecisionTreeClassificationModel(override val model: DecisionTreeModel)
//  extends SparkRegressionModel[DecisionTreeModel]

