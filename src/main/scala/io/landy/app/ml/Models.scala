package io.landy.app.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, RandomForestModel}
import org.apache.spark.rdd.RDD

import scala.language.{existentials, reflectiveCalls}
import scala.pickling.pickler.PrimitivePicklers
import scala.pickling.{Pickle, PickleFormat, directSubclasses}

import io.landy.app.util.arrayOps


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


sealed trait SparkModel[+T <: SparkModel.Model] {
  protected val model:  T
  protected val x:      SparkModel.Extractor

  // TODO(kudinkin): Purge `reflective`-call(s) by narrowing down model-types explicitly

  protected def predictFor(seq: Seq[Double]): Double =
    model.predict(Vectors.dense(x(seq).toArray))
}

/**
  * Model mimic'ing degenerate case of pessimistic oracle,
  * de-marking the baseline equal to the conversion-rate inside the sample
  */
object Baseline extends Serializable {
  def predict(features: Vector): Double = 0.0
  def predict(ps: RDD[Vector]): RDD[Double] = ps.map(predict)
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
          case (v, i) =>

            // We still can see _new_ value of the particular feature
            // that was treated as categorical one during training
            //
            // Is there any other 'graceful' fallback instead random-guess-replace
            // we're replacing this particular feature value with the one of the seen
            // at random
            if (!mapping.contains(i))
              v
            else if (!mapping(i).contains(v))
              mapping(i).values.toArray.random.get
            else
              mapping(i)(v)
        }
        .toArray
  }

}

/**
  * Spark's MLLib abstract regression-model facade
  *
  * @param model  target model
  * @param x      feature-extractor
  * @tparam T     type of the target model
  */
class SparkRegressionModel[+T <: SparkModel.Model](
  override val model: T,
  override val x:     SparkModel.Extractor
) extends RegressionModel
  with    SparkModel[T] {

  override def predict(seq: Seq[Double]): Double = predictFor(seq)

}

/**
  * Spark's MLLib _regression_ decision-tree-model facade
  *
  * @param model  target decision-tree-model
  * @param x      see @SparkRegressionModel for details
  */
final case class SparkDecisionTreeRegressionModel(
  override val model: DecisionTreeModel,
  override val x: SparkModel.Extractor
)
  extends SparkRegressionModel[DecisionTreeModel](model, x)
  with    PickleableModel

/**
  * Spark's MLLib _regression_ random-forest-model facade
  *
  * @param model  target random-forest-model
  * @param x      see @SparkRegressionModel for details
  */
final case class SparkRandomForestRegressionModel(
  override val model: RandomForestModel,
  override val x:     SparkModel.Extractor
)
  extends SparkRegressionModel[RandomForestModel](model, x)
  with    PickleableModel


class SparkClassificationModel[+T <: SparkModel.Model](
  override val model: T,
  override val x:     SparkModel.Extractor
) extends ClassificationModel
  with    SparkModel[T] {

  override def predict(seq: Seq[Double]): Int = predictFor(seq).toInt

}

/**
  * Spark's MLLib _classification_ decision-tree-model facade
  *
  * @param model  target decision-tree-model
  * @param x      see @SparkRegressionModel for details
  */
final case class SparkDecisionTreeClassificationModel(
  override val model: DecisionTreeModel,
  override val x:     SparkModel.Extractor
)
  extends SparkClassificationModel[DecisionTreeModel](model, x)
  with    PickleableModel

/**
  * Spark's MLLib _classification_ random-forest-model facade
  *
  * @param model  target random-forest-model
  * @param x      see @SparkRegressionModel for details
  */
final case class SparkRandomForestClassificationModel(
  override val model: RandomForestModel,
  override val x:     SparkModel.Extractor
)
  extends SparkClassificationModel[RandomForestModel](model, x)
  with    PickleableModel


object Models {

  /**
    * Target model-types system is capable to fit to
    */
  object Types extends Enumeration {
    type Type = Value
    val DecisionTree, RandomForest = Value
  }

  /**
    * Picklers necessary to properly serialized existing models
    */
  object Picklers extends PrimitivePicklers {

    //
    // NOTA BENE
    //    That's inevitable evil: `pickling` can't live with enums (even Scala's ones)
    //
    //    https://github.com/scala/pickling/issues/17
    //

    abstract class AbstractPicklerUnpickler[T]() extends scala.AnyRef with scala.pickling.Pickler[T] with scala.pickling.Unpickler[T] {
      import scala.pickling.{PBuilder, Pickler}

      def putInto[F](field: F, builder: PBuilder)(implicit pickler: Pickler[F]): Unit = {
        pickler.pickle(field, builder)
      }
    }

    import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}

    implicit val algoPickler = new AbstractPicklerUnpickler[Algo.Algo] {
      import scala.pickling.{FastTypeTag, PBuilder, PReader}

      override def tag = FastTypeTag[Algo.Algo]

      override def pickle(picklee: Algo.Algo, builder: PBuilder): Unit = {
        builder.beginEntry(picklee, tag)

        builder.putField("algo", { fb =>
          putInto(picklee match {
            case Algo.Classification  => "classification"
            case Algo.Regression      => "regression"
          }, fb)
        })

        builder.endEntry()
      }

      override def unpickle(tag: String, reader: PReader): Any = {
        stringPickler.unpickleEntry(reader.readField("algo")).asInstanceOf[String] match {
          case "classification" => Algo.Classification
          case "regression"     => Algo.Regression
        }
      }
    }

    implicit val featureTypePickler = new AbstractPicklerUnpickler[FeatureType.FeatureType] {
      import scala.pickling.{FastTypeTag, PBuilder, PReader}

      override def tag = FastTypeTag[FeatureType.FeatureType]

      override def pickle(picklee: FeatureType.FeatureType, builder: PBuilder): Unit = {
        builder.beginEntry(picklee, tag)

        builder.putField("featureType", { fb =>
          putInto(picklee match {
            case FeatureType.Categorical  => "categorical"
            case FeatureType.Continuous   => "continuous"
          }, fb)
        })

        builder.endEntry()
      }

      override def unpickle(tag: String, reader: PReader): Any = {
        stringPickler.unpickleEntry(reader.readField("featureType")).asInstanceOf[String] match {
          case "categorical"  => FeatureType.Categorical
          case "continuous"   => FeatureType.Continuous
        }
      }
    }

    import scala.pickling.Defaults._
    import scala.pickling.Pickler

    implicit val decisionTreeCM = Pickler.generate[SparkDecisionTreeClassificationModel]
    implicit val decisionTreeRM = Pickler.generate[SparkDecisionTreeRegressionModel]

    implicit val randomForestCM = Pickler.generate[SparkRandomForestClassificationModel]
    implicit val randomForestRM = Pickler.generate[SparkRandomForestRegressionModel]

    def pickle(model: PickleableModel)(implicit format: PickleFormat): format.PickleType =
      model match {
        case decisionTreeCM: SparkDecisionTreeClassificationModel => decisionTreeCM.pickle
        case decisionTreeRM: SparkDecisionTreeRegressionModel     => decisionTreeRM.pickle
        case randomForestCM: SparkRandomForestClassificationModel => randomForestCM.pickle
        case randomForestRM: SparkRandomForestRegressionModel     => randomForestRM.pickle
      }

    import scala.pickling.Unpickler

    implicit val pickleableModel = Unpickler.generate[PickleableModel]

    def unpickle(pickle: Pickle)(implicit format: PickleFormat): PickleableModel =
      pickle.unpickle[PickleableModel](pickleableModel, format)

  }

}