package com.flp.control.storage

import akka.pattern.pipe
import com.flp.control.akka.ExecutingActor
import com.flp.control.instance._
import com.flp.control.model._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.mllib.tree.model.DecisionTreeModel

import scala.language.implicitConversions

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

      override def write(t: StartEvent) =
        BSONDocument(
          `appId`     -> t.appId,
          `type`      -> `type:Start`,
          `timestamp` -> t.timestamp,
          `session`   -> t.session,
          `identity`  -> BSON.writeDocument(t.identity)(userIdentityBSON),
          `variation` -> BSON.write(t.variation)(variationBSON)
        )

      override def read(bson: BSONDocument) =
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

  implicit val modelBSON =
    new BSONReader[BSONString, AppInstanceConfig.Model] with BSONWriter[AppInstanceConfig.Model, BSONString] {

      //
      // TODO(kudinkin): Disabled until scala-pickling-0.11.* arrives
      //

      import scala.pickling._
      import Defaults._
      import json._

      //implicit val eitherPickler = Pickler.generate[AppInstanceConfig.Model]

      override def write(model: AppInstanceConfig.Model): BSONString =
        BSONString(
          model.flatMap {
            case Left(e)  => Some(e.asInstanceOf[SparkDecisionTreeClassificationModel].pickle)
            case Right(e) => Some(e.asInstanceOf[SparkDecisionTreeRegressionModel].pickle)
          }.pickle.value
        )

      override def read(bson: BSONString): AppInstanceConfig.Model =
        bson.value.unpickle[Option[String]].map {
          case m =>
            m .unpickle[SparkModel[DecisionTreeModel]] match {
              case c @ SparkDecisionTreeClassificationModel(_)  => Left(c)
              case r @ SparkDecisionTreeRegressionModel(_)      => Right(r)
              case _ =>
                throw new UnsupportedOperationException()
            }
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
            ds    <- bson.getAs[Seq[UserDataDescriptor]]  (`descriptors`);
            model <- bson.getAs[AppInstanceConfig.Model]  (`model`)
          ) yield AppInstanceConfig(
              variations          = vs.toList,
              userDataDescriptors = ds.toList,
              model               = model
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