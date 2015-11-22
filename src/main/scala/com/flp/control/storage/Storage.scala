package com.flp.control.storage

import akka.pattern.pipe
import com.flp.control.akka.ExecutingActor
import com.flp.control.instance._
import com.flp.control.model._
import com.typesafe.config.{Config, ConfigFactory}

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

  implicit val userIdentityBSONSerializer =
    new BSONDocumentReader[UserIdentity] with BSONDocumentWriter[UserIdentity] {

      override def write(uid: UserIdentity): BSONDocument =
        BSON.writeDocument(uid.params)

      override def read(bson: BSONDocument): UserIdentity =
        bson.asOpt[UserIdentity.Params] collect { case ps => UserIdentity(ps) } getOrElse UserIdentity.empty
    }

  implicit val variationBSONSerializer =
    new BSONReader[BSONString, Variation] with BSONWriter[Variation, BSONString] {

      override def write(v: Variation): BSONString =
        BSON.write(v.id)

      override def read(bson: BSONString): Variation =
        Variation(bson.value)
    }

  implicit val userDataDescriptorBSONSerializer =
    new BSONReader[BSONString, UserDataDescriptor] with BSONWriter[UserDataDescriptor, BSONString] {

      override def write(v: UserDataDescriptor): BSONString =
        BSON.write(v.name)

      override def read(bson: BSONString): UserDataDescriptor =
        UserDataDescriptor(bson.value)
    }

  // TODO(kudinkin): Move persisters to actual classes they serialize

  implicit val startEventBSONSerializer =
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

  implicit val finishEventBSONSerializer =
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


  object Model {

    abstract class AbstractPicklerUnpickler[T]() extends scala.AnyRef with scala.pickling.Pickler[T] with scala.pickling.Unpickler[T] {
      import scala.pickling.{PBuilder, Pickler}

      def putInto[F](field: F, builder: PBuilder)(implicit pickler: Pickler[F]): Unit = {
        pickler.pickle(field, builder)
      }
    }


    //
    // NOTA BENE
    //    That's inevitable evil: `pickling` can't live with enums (even Scala's ones)
    //    https://github.com/scala/pickling/issues/17
    //

    import scala.pickling.pickler._

    object Picklers extends PrimitivePicklers {

      import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}

      implicit val algoPickler = new AbstractPicklerUnpickler[Algo.Algo] {
        import scala.pickling.{PBuilder, PReader}

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
        import scala.pickling.{PBuilder, PReader}

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
    }


    import Picklers.{featureTypePickler, algoPickler}

    implicit val binaryBSONSerializer =
      new BSONReader[BSONBinary, AppInstanceConfig.Model] with BSONWriter[AppInstanceConfig.Model, BSONBinary] {

        import scala.pickling._
        import Defaults._
        import binary._

        override def write(model: AppInstanceConfig.Model): BSONBinary =
          BSONBinary(
            model match {
              case Left(e)  => e.asInstanceOf[SparkDecisionTreeClassificationModel].pickle.value
              case Right(e) => e.asInstanceOf[SparkDecisionTreeRegressionModel].pickle.value
            },
            Subtype.UserDefinedSubtype
          )

        override def read(bson: BSONBinary): AppInstanceConfig.Model =
          bson.byteArray.unpickle[PickleableModel] match {
            case c @ SparkDecisionTreeClassificationModel(_)  => Left(c)
            case r @ SparkDecisionTreeRegressionModel(_)      => Right(r)
            case x =>
              throw new UnsupportedOperationException()
          }
      }

    implicit val jsonBSONSerializer =
        new BSONReader[BSONString, AppInstanceConfig.Model] with BSONWriter[AppInstanceConfig.Model, BSONString] {

        import scala.pickling._
        import Defaults._
        import json._

        override def write(model: AppInstanceConfig.Model): BSONString =
          BSONString(
            model match {
              case Left(e)  => e.asInstanceOf[SparkDecisionTreeClassificationModel].pickle.value
              case Right(e) => e.asInstanceOf[SparkDecisionTreeRegressionModel].pickle.value
            }
          )

        override def read(bson: BSONString): AppInstanceConfig.Model =
          bson.value.unpickle[PickleableModel] match {
            case c @ SparkDecisionTreeClassificationModel(_)  => Left(c)
            case r @ SparkDecisionTreeRegressionModel(_)      => Right(r)
            case x =>
              throw new UnsupportedOperationException()
          }
      }
  }

  import Model.binaryBSONSerializer


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

  implicit val appInstanceConfigRecordBSONSerializer =
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