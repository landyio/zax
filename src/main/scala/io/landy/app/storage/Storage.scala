package io.landy.app.storage

import akka.pattern.pipe
import io.landy.app.actors.ExecutingActor
import io.landy.app.instance.Instance.State
import io.landy.app.instance._
import io.landy.app.model._
import com.typesafe.config.{Config, ConfigFactory}
import reactivemongo.bson.DefaultBSONHandlers

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.Failure

class StorageActor extends ExecutingActor {

  import Storage._
  import reactivemongo.api._
  import reactivemongo.api.collections.bson._
  import reactivemongo.bson._

  private val config: Config = ConfigFactory.load()

  private val nodes: Seq[String]  = config.getString("landy.mongo.hosts").split(",").toSeq.map { s => s.trim }
  private val db: String          = config.getString("landy.mongo.database")

  private val driver: MongoDriver = new MongoDriver()

  private val connection: MongoConnection = driver.connection(nodes = nodes)

  private val database: DefaultDB = connection.db(name = db)

  private def trace(f: Future[_], message: => String) =
    f.andThen {
      case Failure(t) => log.error(t, message)
    }

  /**
    * Looks up particular collection inside database
    *
    * @param c collection to bee sought for
    * @return found collection
    */
  private def find(c: String): BSONCollection =
    database.collection[BSONCollection](c)

  import Commands._
  import Persisters._

  def receive = trace {

    case LoadRequest(persister, selector, upTo) =>
      implicit val reader = persister

      // TODO(kudinkin): `collect` silently swallows the errors in the `Storage`

      val c = find(persister.collection)
      val r = c .find(selector)
                .cursor[persister.Target](ReadPreference.Nearest(filterTag = None))
                .collect[List](upTo, stopOnError = false)

      trace(
        r.map { os =>
          // _DBG
          println(s"XYXY: ${os} / ${classOf[persister.Target].getName} / ${persister.collection}")

          Commands.LoadResponse(os)
        },
        s"Failed to retrieve data from ${persister.collection} for request: ${selector}"
      ) pipeTo sender()


    case CountRequest(persister, selector) =>
      val c = find(persister.collection)
      val r = c.count(selector = Some(selector))

      trace(
        r.map { o => Commands.CountResponse(o) },
        s"Failed to retrieve data from ${persister.collection} for request: ${selector}"
      ) pipeTo sender()

    case StoreRequest(persister, obj) =>
      val c = find(persister.collection)
      val r = c.insert(obj)(writer = persister, ec = executionContext)

      trace(
        r.map { r => Commands.StoreResponse(r.ok) },
        s"Failed to save data in ${persister.collection} for object: ${obj}"
      ) pipeTo sender()

    case UpdateRequest(persister, selector, modifier) =>
      val w = Storage.BSONDocumentIdentity

      val c = find(persister.collection)
      val r = c.update(selector, BSONDocument("$set" -> modifier))(selectorWriter = w, updateWriter = w, ec = executionContext)

      trace(
        r.map { res => Commands.UpdateResponse(res.ok) },
        s"Failed to update data in ${persister.collection} with ${modifier} for object: ${selector}"
      ) pipeTo sender()

  }
}

object Storage extends DefaultBSONHandlers {

  val actorName: String = "storage"

  type Id = String

  val `_id` = "_id"
  val `$oid` = "$oid"

  import reactivemongo.bson._

  /**
    * Helper generating selector for Mongo queries by supplied id of the object
    *
    * @param id object-id
    * @return bson-document (so called 'selector')
    */
  private def byId(id: Id) = BSONDocument(`_id` -> BSONObjectID(id))

  /**
    * Commands accepted by the storaging actor
    */
  object Commands {

    import Persisters._

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


  /**
    * Persisters
    */

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

  object Persisters {

    import reactivemongo.bson._

    /**
      * Helpers facilitating collections marshalling/unmarshalling
      */
    object Helpers {
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
    }

    /**
      * Application model persisters instances
      */

    import Helpers._

    // TODO(kudinkin): Move persisters closer to actual classes they serialize

    implicit val userIdentityPersister =
      new BSONDocumentReader[UserIdentity] with BSONDocumentWriter[UserIdentity] {

        override def write(uid: UserIdentity): BSONDocument =
          BSON.writeDocument(uid.params)

        override def read(bson: BSONDocument): UserIdentity =
          bson.asOpt[UserIdentity.Params] collect { case ps => UserIdentity(ps) } getOrElse UserIdentity.empty
      }

    implicit val variationPersister =
      new BSONReader[BSONString, Variation] with BSONWriter[Variation, BSONString] {

        override def write(v: Variation): BSONString =
          BSON.write(v.id)

        override def read(bson: BSONString): Variation =
          Variation(bson.value)
      }

    implicit val userDataDescriptorPersister =
      new BSONDocumentReader[UserDataDescriptor] with BSONDocumentWriter[UserDataDescriptor] {

        override def write(d: UserDataDescriptor): BSONDocument =
          BSONDocument(
            UserDataDescriptor.`name`         -> d.name,
            UserDataDescriptor.`categorical`  -> d.categorical
          )

        override def read(bson: BSONDocument): UserDataDescriptor =
          UserDataDescriptor(
            bson.getAs[String]  (UserDataDescriptor.`name`)         .get,
            bson.getAs[Boolean] (UserDataDescriptor.`categorical`)  .get
          )
      }


    implicit val startEventPersister =
      new Persister[StartEvent] {
        import Event._
        val collection: String = s"events_${`type:Start`}"

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

    implicit val finishEventPersister =
      new Persister[FinishEvent] {
        import Event._
        override val collection: String = s"events_${`type:Finish`}"

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

      //
      // NOTA BENE
      //    That's inevitable evil: `pickling` can't live with enums (even Scala's ones)
      //
      //    https://github.com/scala/pickling/issues/17
      //

      import scala.pickling.pickler._

      object Picklers extends PrimitivePicklers {

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
      }


      import Picklers.{featureTypePickler, algoPickler}

      implicit val binaryPersister =
        new BSONReader[BSONBinary, Instance.Config.Model] with BSONWriter[Instance.Config.Model, BSONBinary] {

          import scala.pickling.Defaults._
          import scala.pickling.binary._

          override def write(model: Instance.Config.Model): BSONBinary =
            BSONBinary(
              model match {
                case Left(e)  => e.asInstanceOf[SparkDecisionTreeClassificationModel].pickle.value
                case Right(e) => e.asInstanceOf[SparkDecisionTreeRegressionModel].pickle.value
              },
              Subtype.UserDefinedSubtype
            )

          override def read(bson: BSONBinary): Instance.Config.Model =
            bson.byteArray.unpickle[PickleableModel] match {
              case c @ SparkDecisionTreeClassificationModel(_, _) => Left(c)
              case r @ SparkDecisionTreeRegressionModel(_, _)     => Right(r)
              case x =>
                throw new UnsupportedOperationException()
            }
        }

      implicit val jsonPersister =
          new BSONReader[BSONString, Instance.Config.Model] with BSONWriter[Instance.Config.Model, BSONString] {

          import scala.pickling.Defaults._
          import scala.pickling.json._

          override def write(model: Instance.Config.Model): BSONString =
            BSONString(
              model match {
                case Left(e)  => e.asInstanceOf[SparkDecisionTreeClassificationModel].pickle.value
                case Right(e) => e.asInstanceOf[SparkDecisionTreeRegressionModel].pickle.value
              }
            )

          override def read(bson: BSONString): Instance.Config.Model =
            bson.value.unpickle[PickleableModel] match {
              case c @ SparkDecisionTreeClassificationModel(_, _) => Left(c)
              case r @ SparkDecisionTreeRegressionModel(_, _)     => Right(r)
              case x =>
                throw new UnsupportedOperationException()
            }
        }
    }

    implicit val instanceStatePersister =
      new BSONDocumentReader[Instance.State] with BSONDocumentWriter[Instance.State] {

        override def write(s: State): BSONDocument =
          s match {
            case State.Training
               | State.Suspended
               | State.NoData =>
              BSONDocument(
                State.`name` -> s.typeName
              )

            case State.Predicting(from) =>
              BSONDocument(
                State.`name`            -> State.Predicting.typeName,
                State.Predicting.`from` -> from.ts
              )
          }

        override def read(bson: BSONDocument): State = {
          import Instance.Epoch

          bson.getAs[String](State.`name`).collect {
            case State.Predicting.typeName =>
              State.Predicting(
                bson.getAs[Long](State.Predicting.`from`) map { ts => Epoch(ts) } getOrElse Epoch.anteChristum
              )

            case n: String =>
              State.withName(n)
          }.get
        }
      }

    //
    // TODO(kudinkin): Merge
    //

    implicit val instanceConfigPersister =
      new BSONDocumentReader[Instance.Config] with BSONDocumentWriter[Instance.Config] {
        import Instance.Config._

        import Model.binaryPersister

        override def write(c: Instance.Config): BSONDocument = {
          BSONDocument(
            `variations`  -> c.variations,
            `descriptors` -> c.userDataDescriptors,
            `model`       -> c.model
          )
        }

        override def read(bson: BSONDocument): Instance.Config = {
          { for (
              vs    <- bson.getAs[Seq[Variation]]           (`variations`);
              ds    <- bson.getAs[Seq[UserDataDescriptor]]  (`descriptors`)
            ) yield Instance.Config(
                variations          = vs.toList,
                userDataDescriptors = ds.toList,
                model               = bson.getAs[Instance.Config.Model](`model`)
              )
          }.get
        }
      }

    implicit val instanceRecordPersister =
      new Persister[Instance.Record] {
        import Instance.Record._
        val collection: String = "instances"

        override def write(t: Instance.Record): BSONDocument =
          BSONDocument(
            `_id`       -> BSONObjectID(t.appId),
            `runState`  -> BSON.writeDocument(t.runState),
            `config`    -> BSON.writeDocument(t.config)
          )

        override def read(bson: BSONDocument): Instance.Record =
          Instance.Record(
            appId     = bson.getAs[BSONObjectID]      (`_id`)      .map { i => i.stringify } getOrElse { "" },
            runState  = bson.getAs[State]             (`runState`) .get, // .getOrElse { AppInstance.State.Suspended },
            config    = bson.getAs[Instance.Config] (`config`)   .get
          )
      }

  }
}