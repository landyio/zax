package com.flp.control.storage

import akka.pattern.pipe
import com.flp.control.akka.ExecutingActor
import com.flp.control.instance.AppInstanceConfig._
import com.flp.control.instance._
import com.flp.control.model._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.configuration.FeatureType.FeatureType
import org.apache.spark.mllib.tree.configuration.{FeatureType, Algo}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, InformationGainStats, Predict, Split}

import scala.language.implicitConversions
import scala.util.Failure

class StorageActor extends ExecutingActor {

  import Storage._
  import reactivemongo.api._
  import reactivemongo.api.collections.bson._
  import reactivemongo.bson._

  private val config: Config = ConfigFactory.load()
  private val nodes: Seq[String] = config.getString("flp.mongo.hosts").split(",").toSeq.map { s => s.trim }
  private val dbname: String = config.getString("flp.mongo.database")

  private val driver: MongoDriver = new MongoDriver()
  private val connection: MongoConnection = driver.connection(nodes = nodes)
  private val database: DefaultDB = connection.db(name = dbname)

  // TODO: think about why the following is not a good idea:
  // TODO: val collections = scala.collection.mutable.Map[String, BSONCollection]()
  // TODO: def find(p: Persister[_]): BSONCollection = collections.getOrElseUpdate( p.collection, database.collection[BSONCollection](p.collection) )
  private def find(c: String): BSONCollection = database.collection[BSONCollection](c)

  def receive = trace {

    case Commands.LoadRequest(persister, selector, upTo) =>
      implicit val reader = persister

      // TODO(kudinkin): `collect` silently swallows the errors in the `Storage`

      val c = find(persister.collection)
      val r = c .find(selector)
                .cursor[persister.Target](ReadPreference.Nearest(filterTag = None))
                .collect[List](upTo, stopOnError = false)

      r.map { os => Commands.LoadResponse(os) } pipeTo sender()


    case Commands.CountRequest(persister, selector) =>
      val c = find(persister.collection)
      val r = c.count(selector = Some(selector))

      r.map { o => Commands.CountResponse(o) } pipeTo sender()

    case Commands.StoreRequest(persister, obj) =>
      val c = find(persister.collection)
      val r = c.insert(obj)(writer = persister, ec = executionContext)

      r.map { res => Commands.StoreResponse(res.ok) } pipeTo sender()

    case Commands.UpdateRequest(persister, selector, modifier) =>
      val w = Storage.BSONDocumentIdentity

      val c = find(persister.collection)
      val r = c.update(selector, BSONDocument("$set" -> modifier))(selectorWriter = w, updateWriter = w, ec = executionContext)

      r.map { res => Commands.UpdateResponse(res.ok) } pipeTo sender()

  }

  // override def preStart(): Unit = { }
  // override def postStop(): Unit = { }

}

object Storage extends reactivemongo.bson.DefaultBSONHandlers {

  val actorName: String = "storage"

  type Id = String
  val `_id` = "_id"
  val `$oid` = "$oid"

  import reactivemongo.bson._

  def byId(id: Id) = BSONDocument(`_id` -> BSONObjectID(id))

  object Commands {

    case class StoreRequest[T](persister: PersisterW[T], obj: T)
    case class StoreResponse(ok: Boolean)

    object Store {
      def apply[T](obj: T)(implicit persister: PersisterW[T]): StoreRequest[T] = StoreRequest[T](persister, obj)
    }

    case class LoadRequest  [T](persister: PersisterR[T], selector: BSONDocument, maxSize: Int)
    case class LoadResponse [T](seq: Seq[T])

    object Load {

      def apply[T](id: Id)(implicit persister: PersisterR[T]) =
        LoadRequest[T](persister, byId(id), 1)

      def apply[T](selector: Producer[(String, BSONValue)]*)(maxSize: Int = Int.MaxValue)(implicit persister: PersisterR[T]) =
        LoadRequest[T](persister, BSONDocument(selector:_*), maxSize)

    }

    case class UpdateRequest[T](persister: PersisterBase[T], selector: BSONDocument, modifier: BSONDocument)
    case class UpdateResponse(ok: Boolean)

    object Update {

      def apply[T](selector: Producer[(String, BSONValue)]*)(modifier: Producer[(String, BSONValue)]*)(implicit persister: PersisterBase[T]) =
        UpdateRequest[T](
          persister = persister,
          selector = BSONDocument(selector:_*),
          modifier = BSONDocument(modifier:_*)
        )

      def apply[T](id: Id)(modifier: Producer[(String, BSONValue)]*)(implicit persister: PersisterBase[T]) =
        UpdateRequest[T](
          persister = persister,
          selector = byId(id),
          modifier = BSONDocument(modifier:_*)
        )
    }

    case class CountRequest[T](persister: PersisterBase[T], selector: BSONDocument)
    case class CountResponse(count: Int)

    object Count {
      def apply[T](elements: Producer[(String, BSONValue)]*)(implicit persister: PersisterBase[T]) = CountRequest[T](
        persister = persister,
        selector = BSONDocument(elements:_*)
      )
    }

  }

  import reactivemongo.bson._

  implicit def MapReader[B <: BSONValue, V](implicit reader: BSONReader[B, V]): BSONDocumentReader[Map[String, V]] =
    new BSONDocumentReader[Map[String, V]] {
      def read(bson: BSONDocument): Map[String, V] = bson.elements.map { case (k, v) => k -> reader.read(v.asInstanceOf[B]) }.toMap
    }

  implicit def MapWriter[V, B <: BSONValue](implicit writer: BSONWriter[V, B]): BSONDocumentWriter[Map[String, V]] =
    new BSONDocumentWriter[Map[String, V]] {
      def write(map: Map[String, V]): BSONDocument = BSONDocument(map.toStream.map { case (k, v) => k -> writer.write(v) })
    }

  implicit def SeqReader[B <: BSONValue, V](implicit reader: BSONReader[B, V]): BSONReader[BSONArray, Seq[V]] =
    new BSONReader[BSONArray, Seq[V]] {
      def read(bson: BSONArray): Seq[V] = bson.values.map { case v => reader.read(v.asInstanceOf[B]) }.toSeq
    }

  implicit def SeqWriter[V, B <: BSONValue](implicit writer: BSONWriter[V, B]): BSONWriter[Seq[V], BSONArray] =
    new BSONWriter[Seq[V], BSONArray] {
      def write(seq: Seq[V]): BSONArray = BSONArray(seq.toStream.map { case (v) => writer.write(v) })
    }

  trait PersisterBase[T] {
    type Target = T

    val collection: String
  }

  trait PersisterR[T] extends PersisterBase[T] with BSONDocumentReader[T] {
    override def toString: String = s"persister-r($collection)"
  }

  trait PersisterW[T] extends PersisterBase[T] with BSONDocumentWriter[T] {
    override def toString: String = s"persister-w($collection)"
  }

  trait Persister[T] extends PersisterR[T] with PersisterW[T] {
    override def toString: String = s"persister-rw($collection)"
  }


  //
  // Persisters
  //

  implicit val userIdentityBSON =
    new BSONDocumentReader[UserIdentity] with BSONDocumentWriter[UserIdentity] {

      override def write(uid: UserIdentity): BSONDocument =
        BSON.writeDocument(uid.params)

      override def read(bson: BSONDocument): UserIdentity =
        bson.asOpt[UserIdentity.Params] collect { case ps => UserIdentity(ps) } getOrElse UserIdentity.empty
    }

  implicit val variationBSON =
    new BSONReader[BSONString, Variation] with BSONWriter[Variation, BSONString] {

      override def write(v: Variation): BSONString =
        BSON.write(v.id)

      override def read(bson: BSONString): Variation =
        Variation(bson.value)
    }

  implicit val userDataDescriptorBSON =
    new BSONReader[BSONString, UserDataDescriptor] with BSONWriter[UserDataDescriptor, BSONString] {

      override def write(v: UserDataDescriptor): BSONString =
        BSON.write(v.name)

      override def read(bson: BSONString): UserDataDescriptor =
        UserDataDescriptor(bson.value)
    }

  // TODO(kudinkin): Move persisters to actual classes they serialize

  implicit val startEventBSON =
    new Persister[StartEvent] {
      import Event._
      val collection: String = s"events:${`type:Start`}"

      override def write(t: StartEvent): BSONDocument =
        BSONDocument(
          `appId`     -> t.appId,
          `type`      -> `type:Start`,
          `timestamp` -> t.timestamp,
          `session`   -> t.session,
          `identity`  -> BSON.writeDocument(t.identity),
          `variation` -> BSON.write(t.variation)
        )

      override def read(bson: BSONDocument): StartEvent =
        StartEvent(
          appId     = bson.getAs[String]        (`appId`)     .get,
          session   = bson.getAs[String]        (`session`)   .get,
          timestamp = bson.getAs[Long]          (`timestamp`) .get,
          identity  = bson.getAs[UserIdentity]  (`identity`)  .get,
          variation = bson.getAs[Variation]     (`variation`) .get
        )
    }

  implicit val finishEventBSON =
    new Persister[FinishEvent] {
      import Event._
      override val collection: String = s"events${`type:Finish`}"

      override def write(t: FinishEvent) =
        BSONDocument(
          `appId`     -> t.appId,
          `type`      -> `type:Finish`,
          `timestamp` -> t.timestamp,
          `session`   -> t.session
        )

      override def read(bson: BSONDocument) =
        FinishEvent(
          appId     = bson.getAs[String]  (`appId`)     .get,
          session   = bson.getAs[String]  (`session`)   .get,
          timestamp = bson.getAs[Long]    (`timestamp`) .get
        )
    }

  import scala.pickling.{PBuilder, Pickler}

  abstract class AbstractPicklerUnpickler[T]() extends scala.AnyRef with scala.pickling.Pickler[T] with scala.pickling.Unpickler[T] {
    def putInto[T](field: T, builder: PBuilder)(implicit pickler: Pickler[T]): Unit = {
      builder.hintTag(pickler.tag)
      pickler.pickle(field, builder)
    }
  }


  implicit val modelBSON =
    new BSONReader[BSONString, AppInstanceConfig.Model] with BSONWriter[AppInstanceConfig.Model, BSONString] {

      //
      // TODO(kudinkin): Disabled until scala-pickling-0.11.* arrives
      //

      import sbt.serialization.pickler.OptionPicklers
      import scala.pickling.pickler.PrimitivePicklers

      object Picklers extends PrimitivePicklers with OptionPicklers {

        import scala.pickling.{AbstractPicklerUnpickler => _, _}
        import Defaults._

        //implicit val eitherPickler = Pickler.generate[AppInstanceConfig.Model]

        //
        // NB:
        //  That's a hack to overcome SI-9306 (https://issues.scala-lang.org/browse/SI-9306)
        //

        val stringOptPickler = optionPickler[String]

        val statsPickler      = optionPickler(FastTypeTag[InformationGainStats], implicitly[Pickler[InformationGainStats]], implicitly[Unpickler[InformationGainStats]], FastTypeTag[Option[InformationGainStats]])

        val predictPickler    = implicitly[Pickler[Predict]]
        val predictUnpickler  = implicitly[Unpickler[Predict]]


        implicit val featureTypePickler = new AbstractPicklerUnpickler[FeatureType.FeatureType] {
          override def tag = FastTypeTag[FeatureType]

          override def pickle(picklee: FeatureType, builder: PBuilder): Unit = {
            builder.hintTag(tag)
            builder.beginEntry(picklee)

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


        implicit val splitPickler = new AbstractPicklerUnpickler[Split] {
          override def tag = FastTypeTag[Split]

          implicit val listDoublePickler    = implicitly[Pickler[List[Double]]]
          implicit val listDoubleUnpickler  = implicitly[Unpickler[List[Double]]]

          override def pickle(picklee: Split, builder: PBuilder): Unit = {
            builder.hintTag(tag)
            builder.beginEntry(picklee)

            builder.putField("feature", { fb =>
              putInto(picklee.feature, fb)
            })

            builder.putField("threshold", { fb =>
              putInto(picklee.threshold, fb)
            })

            builder.putField("featureType", { fb =>
              putInto(picklee.featureType, fb)
            })

            builder.putField("categories", { fb =>
              putInto(picklee.categories, fb)
            })

            builder.endEntry()
          }

          override def unpickle(tag: String, reader: PReader): Any = {
            val feature   = intPickler.unpickleEntry(reader.readField("feature")).asInstanceOf[Int]
            val threshold = doublePickler.unpickleEntry(reader.readField("threshold")).asInstanceOf[Double]
            val featureType = featureTypePickler.unpickleEntry(reader.readField("featureType")).asInstanceOf[FeatureType.FeatureType]
            val categories = listDoubleUnpickler.unpickleEntry(reader.readField("categories")).asInstanceOf[List[Double]]

            new Split(feature, threshold, featureType, categories)
          }
        }

        val splitOptPickler = optionPickler(FastTypeTag[Split], splitPickler, splitPickler, FastTypeTag[Option[Split]])

        implicit val decisionTreeNodePickler = new AbstractPicklerUnpickler[org.apache.spark.mllib.tree.model.Node] {

          import org.apache.spark.mllib.tree.model.{InformationGainStats, Node, Predict, Split}

          val selfPickler: Pickler[Option[Node]] with Unpickler[Option[Node]] = optionPickler(tag, this, this, optTag)

          override  def tag     = FastTypeTag[Node]
                    def optTag  = FastTypeTag[Option[Node]]

          override def pickle(picklee: Node, builder: PBuilder): Unit = {
            builder.hintTag(tag)
            builder.beginEntry(picklee)

            println(s"WOWO -BEGIN: ${picklee.id}")

            builder.putField("id", { fb =>
              putInto(picklee.id, fb)
            })

            builder.putField("predict", { fb =>
              putInto(picklee.predict, fb)
            })

            builder.putField("impurity", { fb =>
              putInto(picklee.impurity, fb)
            })

            builder.putField("isLeaf", { fb =>
              putInto(picklee.isLeaf, fb)
            })

            builder.putField("split", { fb =>
              putInto(picklee.split, fb)
            })

            builder.putField("leftNode", { fb =>
              putInto(picklee.leftNode, fb)(selfPickler)
            })

            builder.putField("rightNode", { fb =>
              putInto(picklee.rightNode, fb)(selfPickler)
            })

            builder.putField("stats", { fb =>
              putInto(picklee.stats, fb)
            })

            builder.endEntry()
          }

          override def unpickle(tag: String, reader: PReader): Any = {
            val id        = intPickler        .unpickleEntry(reader.readField("id"))        .asInstanceOf[Int]
            val predict   = predictUnpickler  .unpickleEntry(reader.readField("predict"))   .asInstanceOf[Predict]
            val impurity  = doublePickler     .unpickleEntry(reader.readField("impurity"))  .asInstanceOf[Double]
            val isLeaf    = booleanPickler    .unpickleEntry(reader.readField("isLeaf"))    .asInstanceOf[Boolean]
            val split     = splitOptPickler   .unpickleEntry(reader.readField("split"))     .asInstanceOf[Option[Split]]
            val leftNode  = selfPickler       .unpickleEntry(reader.readField("leftNode"))  .asInstanceOf[Option[Node]]
            val rightNode = selfPickler       .unpickleEntry(reader.readField("rightNode")) .asInstanceOf[Option[Node]]
            val stats     = statsPickler    .unpickleEntry(reader.readField("stats"))     .asInstanceOf[Option[InformationGainStats]]

            println(s"WOWO -END: ${id}")

            new Node(id, predict, impurity, isLeaf, split, leftNode, rightNode, stats)
          }
        }

        implicit val classificatorPickler = Pickler.generate[SparkDecisionTreeClassificationModel]


        implicit val decisionTreePickler = new AbstractPicklerUnpickler[DecisionTreeModel] {
          override def tag = FastTypeTag[DecisionTreeModel]

          val algoPickler = implicitly[Pickler[Algo.Algo]]
          val algoUnpickler = implicitly[Unpickler[Algo.Algo]]

          override def pickle(picklee: DecisionTreeModel, builder: PBuilder): Unit = {
            builder.hintTag(tag)
            builder.beginEntry(picklee)

            builder.putField("topNode", { fb =>
              putInto(picklee.topNode, fb)
            })

            builder.putField("algo", { fb =>
              putInto(picklee.algo, fb)
            })

            builder.endEntry()
          }

          override def unpickle(tag: String, reader: PReader): Any = {
            val topNode = decisionTreeNodePickler.unpickleEntry(reader.readField("topNode")).asInstanceOf[org.apache.spark.mllib.tree.model.Node]
            val algo = algoUnpickler.unpickleEntry(reader.readField("algo")).asInstanceOf[Algo.Algo]

            new DecisionTreeModel(topNode, algo)
          }
        }

        //
        // TODO(kudinkin): report `pickling` being unable to properly diagnose covariant-type
        //
        implicit val regressorPickler = new AbstractPicklerUnpickler[SparkDecisionTreeRegressionModel] {

          override def tag = FastTypeTag[SparkDecisionTreeRegressionModel]

          override def pickle(picklee: SparkDecisionTreeRegressionModel, builder: PBuilder): Unit = {
            builder.hintTag(tag)
            builder.beginEntry(picklee)

            builder.putField("model", { fb =>
              fb.hintTag(decisionTreePickler.tag)
              decisionTreePickler.pickle(picklee.model, fb)
            })

            builder.endEntry()
          }

          override def unpickle(tag: String, reader: PReader): Any = {
            val model = decisionTreePickler.unpickleEntry(reader.readField("model")).asInstanceOf[DecisionTreeModel]
            println(s"BOWOW: ${model}")
            SparkDecisionTreeRegressionModel(model)
          }
        }

      }

      import Picklers._

      import scala.pickling._
      import Defaults._
      import json._

      override def write(model: AppInstanceConfig.Model): BSONString =
        BSONString(
          model match {
            case Left(e)  => e.asInstanceOf[SparkDecisionTreeClassificationModel].pickle(json.pickleFormat, classificatorPickler).value
            case Right(e) => e.asInstanceOf[SparkDecisionTreeRegressionModel]    .pickle(json.pickleFormat, regressorPickler).value
          }
        )


      override def read(bson: BSONString): AppInstanceConfig.Model = {
        // _DBG
        println(s"YOYOYO: ${m}")

        val up = Unpickler.generate[PickleableModel]

        val x = bson.value.unpickle[PickleableModel](up, json.pickleFormat) match {
          case c @ SparkDecisionTreeClassificationModel(_)  => Left(c)
          case r @ SparkDecisionTreeRegressionModel(_)      => Right(r)
          case x =>

            // _DBG
            println(s"YOYOYOY[2]: ${x}")

            throw new UnsupportedOperationException()
        }

        println(s"YOYOYO[3]: ${x}")
        x
    }
  }


  //
  // TODO(kudinkin): Merge
  //

  implicit val appInstanceConfigBSON =
    new BSONDocumentReader[AppInstanceConfig] with BSONDocumentWriter[AppInstanceConfig] {
      import AppInstanceConfig._

      override def write(c: AppInstanceConfig): BSONDocument = {
        BSONDocument(
          `variations`  -> c.variations,
          `descriptors` -> c.userDataDescriptors,
          `model`       -> c.model
        )
      }

      override def read(bson: BSONDocument): AppInstanceConfig = {
        { for (
            vs    <- bson.getAs[Seq[Variation]]           (`variations`);
            ds    <- bson.getAs[Seq[UserDataDescriptor]]  (`descriptors`)
          ) yield AppInstanceConfig(
              variations          = vs.toList,
              userDataDescriptors = ds.toList,
              model               = bson.getAs[AppInstanceConfig.Model](`model`)
            )
        }.get // getOrElse AppInstanceConfig.empty
      }
    }

  implicit val appInstanceConfigRecordBSON =
    new Persister[AppInstanceConfig.Record] {
      import AppInstanceConfig.Record._
      val collection: String = "appconfig"

      override def write(t: AppInstanceConfig.Record): BSONDocument =
        BSONDocument(
          `_id`       -> BSONObjectID(t.appId),
          `runState`  -> BSON.write(t.runState.toString),
          `config`    -> BSON.writeDocument(t.config)
        )

      override def read(bson: BSONDocument): AppInstanceConfig.Record = {
        AppInstanceConfig.Record(
          appId     = bson.getAs[BSONObjectID]      (`_id`)       map { i => i.stringify } getOrElse { "" },
          runState  = bson.getAs[String]            (`runState`)  map { s => AppInstance.State.withName(s) } getOrElse { AppInstance.State.Suspended },
          config    = bson.getAs[AppInstanceConfig] (`config`)   .get
        )
      }
    }
}