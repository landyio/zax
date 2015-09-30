package com.flp.control.instance

import com.flp.control.model.{Variation, IdentityData}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.util.Random

/** */
trait AppPredictor {

  /** { param.key -> param } */
  val params: Map[String, AppParamPredictor]

  //
  def predict(identity: IdentityData.Type): Variation.Type = Variation(
    params
      .par // in-parallel
      .map { case (k, p) => ( k -> p.predict(identity) ) }
      .seq // back-to-normal-map
  )

}

object AppPredictor {
  def apply(params: Map[String, AppParamPredictor]) = new AppPredictorImpl(params)
}

class AppPredictorImpl(
  override val params: Map[String, AppParamPredictor]
) extends AppPredictor


trait UserDataIdentifier {
  val name: String
  val numerize: (Option[String] => Double)
}

object UserDataIdentifier {
  val default: (Option[String] => Double) = ( o => o.map { s => s.hashCode() % 16 } .getOrElse(0) .toDouble )
  def apply(name: String, numerize: (Option[String] => Double) = default) = new UserDataIdentifierImpl(name, numerize)
}

class UserDataIdentifierImpl(
  override val name: String,
  override val numerize: (Option[String] => Double)
) extends UserDataIdentifier


trait AppParamPredictor {

  val variants: Map[Int, String] // { variant.index -> variant.value }
  val userdata: Seq[UserDataIdentifier] // { request parameter }
  var regression: RegressionModel = RegressionModel.empty // regression model

  //
  def predict(identity: IdentityData.Type): String = {
    val factor: Double = 1e-2
    def randomizer(): Double = (factor * Random.nextInt() / Int.MaxValue)
    return variants
      .par // in-parallel
      .toStream
      .map { e => ( probability(identity, e) -> e ) }
      .map { case (p, (k, v)) => ( ( p + randomizer() ) -> v ) }
      .sortBy { case (p, _) => -p }
      .collectFirst { case (_, v) => v }
      .getOrElse { "" }
  }

  //
  def probability(params: Map[String, String], variant: (Int, String)): Double = {
    val (index, _) = variant
    val vector: Seq[Double] = userdata.map { ud => ud.numerize(params.get(ud.name)) } ++ Seq( index.toDouble )
    return regression.predict(vector)
  }

}

object AppParamPredictor {
  def apply(variants: Map[Int, String], userdata: Seq[UserDataIdentifier]) = new AppParamPredictorImpl(variants, userdata)
}

class AppParamPredictorImpl(
  override val variants: Map[Int, String],
  override val userdata: Seq[UserDataIdentifier]
) extends AppParamPredictor


trait RegressionModel {
  def predict(vector: Seq[Double]): Double
}

object RegressionModel {
  val empty: RegressionModel = new RegressionModel {
    override def predict(vector: Seq[Double]): Double = {
      // System.out.println(Thread.currentThread().getName)
      return 0d
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
    return model.predict(v)
  }
}

