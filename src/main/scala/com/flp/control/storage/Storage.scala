package com.flp.control.storage

import akka.pattern.pipe

import com.flp.control.akka.DefaultActor
import com.flp.control.instance._
import com.flp.control.model._
import com.typesafe.config.{ConfigFactory, Config}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.Future

class StorageActor extends DefaultActor {

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
  // TODO: def c(p: Persister[_]): BSONCollection = collections.getOrElseUpdate( p.collection, database.collection[BSONCollection](p.collection) )
  private def c(c: String): BSONCollection = database.collection[BSONCollection](c)

  def receive = trace {

    case Commands.LoadRequest(persister, id) => {
      val collection: BSONCollection = c(persister.collection)
      val future: Future[Option[_]] = collection.find( byId(id) ).one( reader = persister, ec = executionContext )
      pipe( future.map { o => Commands.LoadResponse( o ) } ).to( sender() )
    }

    case Commands.CountRequest(persister, selector) => {
      val collection: BSONCollection = c(persister.collection)
      val future: Future[Int] = collection.count( selector = Some(selector) )
      pipe( future.map { o => Commands.CountResponse( o ) } ).to( sender() )
    }

    case Commands.StoreRequest(persister, obj, asking) => {
      val collection: BSONCollection = c(persister.collection)
      val future: Future[WriteResult] = collection.insert( obj )( writer = persister, ec = executionContext )
      if (asking) {
        pipe( future.map { res => Commands.StoreResponse( res.ok ) } ).to( sender() )
      }
    }

    case Commands.UpdateRequest(persister, selector, modifier, asking) => {
      val collection: BSONCollection = c(persister.collection)
      val future: Future[WriteResult] = collection.update(selector, BSONDocument("$set" -> modifier)) ( selectorWriter = Storage.BSONDocumentIdentity, updateWriter = Storage.BSONDocumentIdentity, ec = executionContext )
      if (asking) {
        pipe( future.map { res => Commands.UpdateResponse( res.ok ) } ).to( sender() )
      }
    }

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

    case class StoreRequest[T](persister: PersisterW[T], obj: T, asking: Boolean)
    case class StoreResponse(ok: Boolean)

    object Store {
      def apply[T](obj: T, asking: Boolean = false)(implicit persister: PersisterW[T]): StoreRequest[T] = StoreRequest[T](persister, obj, asking)
    }

    case class LoadRequest[T](persister: PersisterR[T], id: Id)
    case class LoadResponse[T](obj: Option[T])

    object Load {
      def apply[T](id: Id)(implicit persister: PersisterR[T]) = LoadRequest[T](persister, id)
    }

    case class UpdateRequest[T](persister: PersisterBase[T], selector: BSONDocument, modifier: BSONDocument, asking: Boolean)
    case class UpdateResponse(ok: Boolean)

    object Update {
      def selector[T](selector: Producer[(String, BSONValue)]*)(modifier: Producer[(String, BSONValue)]*)(asking: Boolean = false)(implicit persister: PersisterBase[T]) = UpdateRequest[T](
        persister = persister,
        selector = BSONDocument(selector:_*),
        modifier = BSONDocument(modifier:_*),
        asking = asking
      )
      def id[T](id: Id)(modifier: Producer[(String, BSONValue)]*)(asking: Boolean = false)(implicit persister: PersisterBase[T]) =UpdateRequest[T](
        persister = persister,
        selector = byId(id),
        modifier = BSONDocument(modifier:_*),
        asking = asking
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

  implicit def MapReader[B <: BSONValue, V](implicit reader: BSONReader[B, V]): BSONDocumentReader[Map[String, V]] = new BSONDocumentReader[Map[String, V]] {
    def read(bson: BSONDocument): Map[String, V] = bson.elements.map { case (k, v) => k -> reader.read( v.asInstanceOf[B] ) } .toMap
  }

  implicit def MapWriter[V, B <: BSONValue](implicit writer: BSONWriter[V, B]): BSONDocumentWriter[Map[String, V]] = new BSONDocumentWriter[Map[String, V]] {
    def write(map: Map[String, V]): BSONDocument = BSONDocument( map.toStream.map { case (k, v) => k -> writer.write(v) } )
  }

  implicit def SeqReader[B <: BSONValue, V](implicit reader: BSONReader[B, V]): BSONReader[BSONArray, Seq[V]] = new BSONReader[BSONArray, Seq[V]] {
    def read(bson: BSONArray): Seq[V] = bson.values.map { case v => reader.read( v.asInstanceOf[B] ) } .toSeq
  }

  implicit def SeqWriter[V, B <: BSONValue](implicit writer: BSONWriter[V, B]): BSONWriter[Seq[V], BSONArray] = new BSONWriter[Seq[V], BSONArray] {
    def write(seq: Seq[V]): BSONArray = BSONArray( seq.toStream.map { case (v) => writer.write(v) } )
  }

  implicit val appInstanceConfigBSON = new BSONDocumentReader[AppInstanceConfig] with BSONDocumentWriter[AppInstanceConfig] {
    override def read(bson: BSONDocument): AppInstanceConfig = AppInstanceConfig(BSON.readDocument[Map[String,Seq[String]]]( bson ))
    override def write(t: AppInstanceConfig): BSONDocument = BSON.write(t.variants)
  }

  trait PersisterBase[T] {
    val collection: String
  }

  trait PersisterR[T] extends PersisterBase[T] with BSONDocumentReader[T] {
    override def toString: String = s"persister-r(${collection})"
  }

  trait PersisterW[T] extends PersisterBase[T] with BSONDocumentWriter[T] {
    override def toString: String = s"persister-w(${collection})"
  }

  trait Persister[T] extends PersisterR[T] with PersisterW[T] {
    override def toString: String = s"persister-rw(${collection})"
  }

  implicit val startEventBSON = new PersisterW[StartEvent] {
    import Event._
    override val collection: String = "events"
    override def write(t: StartEvent): BSONDocument = BSONDocument(
      `appId` -> t.appId,
      `type` -> `type:Start`,
      `timestamp` -> t.timestamp,
      `session` -> t.session,
      `identity` -> BSON.writeDocument(t.identity),
      `variation` -> BSON.writeDocument(t.variation)
    )
  }

  implicit val finishEventBSON = new PersisterW[FinishEvent] {
    import Event._
    override val collection: String = "events"
    override def write(t: FinishEvent): BSONDocument = BSONDocument(
      `appId` -> t.appId,
      `type` -> `type:Finish`,
      `timestamp` -> t.timestamp,
      `session` -> t.session
    )
  }

  implicit val appInstanceConfigRecordBSON = new Persister[AppInstanceConfigRecord] {
    override val collection: String = "appconfig"
    override def write(t: AppInstanceConfigRecord): BSONDocument = BSONDocument(
      `_id` -> BSONObjectID( t.appId ),
      AppInstanceConfigRecord.`runState` -> BSON.write( t.runState.toString ),
      AppInstanceConfigRecord.`config` -> BSON.writeDocument(t.config)
    )
    override def read(bson: BSONDocument): AppInstanceConfigRecord = AppInstanceConfigRecord(
      appId = bson.getAs[BSONObjectID]("_id").map { i => i.stringify } getOrElse { "" },
      runState = bson.getAs[String]("runState").map { s => AppInstanceRunState.withName(s) } getOrElse { AppInstanceRunState.Stopped },
      config = bson.getAs[AppInstanceConfig]("config").get
    )
  }

}