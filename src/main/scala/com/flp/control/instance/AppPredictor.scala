package com.flp.control.instance

import com.flp.control.model.{Variation, IdentityData}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.util.Random

trait AppPredictor {

  /** Variations?
    *
    */
  private[instance] val params: Map[String, AppParamPredictor]

  def predict(identity: IdentityData.Type): Variation.Type = Variation(
    params
      .par // in-parallel
      .map { case (k, p) => k -> p.predict(identity) }
      .seq // back-to-normal-map
  )

}

object AppPredictor {
  def apply(params: Map[String, AppParamPredictor]) = new AppPredictorImpl(params)
}

class AppPredictorImpl(
  override val params: Map[String, AppParamPredictor]
) extends AppPredictor


trait UserDataDescriptor {
  val name: String
  val hash: (Option[String] => Double)
}

object UserDataDescriptor {
  private val default = (o: Option[String]) => o.map { _.hashCode() % 16 }.getOrElse(0).toDouble
  def apply(name: String, hash: (Option[String] => Double) = default) =
    new UserDataDescriptorImpl(name, hash)
}

class UserDataDescriptorImpl(
  override val name: String,
  override val hash: (Option[String] => Double)
) extends UserDataDescriptor


trait AppParamPredictor {

  val variations: Map[Int, String] // { variant.index -> variant.value }
  val userDataDescriptors: Seq[UserDataDescriptor] // { request parameter }

  var regression: RegressionModel = RegressionModel.empty // regression model

  def predict(identity: IdentityData.Type): String = {
    val factor: Double = 1e-2
    def rand(): Double = factor * Random.nextInt() / Int.MaxValue

    variations
      .par // in-parallel
      .toStream
      .map { e => probability(identity, e) -> e }
      .map { case (p, (k, v)) => (p + rand()) -> v }
      .sortBy { case (p, _) => -p }
      .collectFirst { case (_, v) => v }
      .getOrElse { "" }
  }

  def probability(params: IdentityData.Type, variant: (Int, String)) =
    variant match {
      case (idx, _) =>
        regression.predict(userDataDescriptors.map { d => d.hash(params.get(d.name)) } ++ Seq(idx.toDouble))
    }

}

object AppParamPredictor {
  def apply(variants: Map[Int, String], userdata: Seq[UserDataDescriptor]) = new AppParamPredictorImpl(variants, userdata)
}

class AppParamPredictorImpl(
                             override val variations: Map[Int, String],
                             override val userDataDescriptors: Seq[UserDataDescriptor]
) extends AppParamPredictor


trait RegressionModel {
  def predict(vector: Seq[Double]): Double
}

object RegressionModel {
  val empty: RegressionModel = new RegressionModel {
    override def predict(vector: Seq[Double]): Double = {
      // System.out.println(Thread.currentThread().getName)
      0d
    }
  }
}

// TODO: use spark:
// * https://spark.apache.org/docs/latest/mllib-ensembles.html
// * https://spark.apache.org/docs/latest/mllib-data-types.html
// * https://spark.apache.org/docs/latest/ml-guide.html
class SparkRegressionModel(
  val model: RandomForestModel // predicted labels are +1 or -1 for GBT.
) extends RegressionModel {
  override def predict(vector: Seq[Double]): Double = {
    val v: DenseVector = new DenseVector( vector.toArray )
    model.predict(v)
  }
}

