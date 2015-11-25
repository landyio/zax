package com.flp.control.instance

import akka.actor.ActorRef
import akka.pattern.pipe
import akka.util.Timeout
import com.flp.control.akka.ExecutingActor
import com.flp.control.boot.Boot
import com.flp.control.model._
import com.flp.control.spark.SparkDriverActor
import com.flp.control.storage.Storage
import com.flp.control.storage.Storage.Commands.{Update, UpdateResponse}
import com.flp.control.util.{Reflect, Identity, boolean2Int}

import scala.collection.BitSet
import scala.compat.Platform
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

import scala.language.{implicitConversions, postfixOps}

class AppInstanceActor(val appId: String) extends ExecutingActor {

  import AppInstance._

  import util.State._

  private val sparkDriverRef = Boot.actor(classOf[SparkDriverActor].getName)

  private var runState: State = State.Loading
  private var predictor: Option[Predictor] = None

  /**
    * Storage
    **/
  private def storage(): ActorRef = Boot.actor(com.flp.control.storage.Storage.actorName)

  /**
    * Predicts 'most-probable' variation
    *
    * @return prediction result (given user-{{{identity}}})
    **/
  private def predict(identity: UserIdentity): Variation = {
    val v = predictor
              .map { p => p.predictFor(identity) }
              .getOrElse {
                Variation.sentinel
              }

    log.debug("Predicted {{}} for {{}} [{{}}]", v, identity, predictor)
    v
  }

  /**
    * Checks whether current app-state allows it to have model
    * trained
    *
    * @return whether app-instance is in eligible state (for training) or not
    */
  private def checkEligibility: Future[Boolean] = {

    val MINIMAL_POSITIVE_OUTCOMES_NO  = 1   /* 25 */
    val MINIMAL_SAMPLE_SIZE           = 10  /* 100 */

    //
    // TODO(kudinkin): We need some policy over here
    //

    runState match {

      case State.Predicting(from) =>
        getStatus(from.ts).map { s =>
          s.eventsAllFinish >= MINIMAL_POSITIVE_OUTCOMES_NO &&
          s.eventsAllStart  >= MINIMAL_SAMPLE_SIZE
        }

      case State.NoData => Future { true }
      case _            => Future { false }
    }
  }

  /**
    * Retrains specified app
    *
    * @return
    */
  private def train(): Future[Option[Double]] = {

    import SparkDriverActor.Commands.{TrainRegressor, TrainRegressorResponse}

    val MAXIMAL_SAMPLE_SIZE = 1000

    checkEligibility.flatMap {

      case true => switchState(State.Training).flatMap { _ =>

          buildTrainingSample(MAXIMAL_SAMPLE_SIZE)
            .flatMap { sample =>
              ask[TrainRegressorResponse](
                sparkDriverRef,
                explain(sample) match { case (s, cats) => TrainRegressor(s, cats) }
              )(executionContext, Commands.trainTimeout)
            }
            .map {
              case TrainRegressorResponse(model, error) =>

                import Storage.{appInstanceConfigBSONSerializer, appInstanceConfigRecordBSONSerializer}

                ask[UpdateResponse](this.storage(), Update[AppInstanceConfig.Record](appId) {
                  AppInstanceConfig.Record.`config` ->
                    getConfig.copy(model = Some(Right(model.asInstanceOf[AppInstanceConfig.RegressionModel])))
                })

                Some(error)
            }
            .andThen {
              case Success(Some(_)) =>
                switchState(State.Predicting(from = Platform.currentTime))

              case Failure(t) =>
                log.error(t, "Training of predictor for #{{}} failed!", appId)
            }
        }

      case false => Future { None }
    }
  }

  // TODO(kudinkin): move?

  private def explain(sample: Seq[((UserIdentity, Variation), Goal#Type)]): (Seq[(Seq[Double], Double)], BitSet) =
    predictor match {
      case Some(p) =>
        val config = p.config
        val s = sample.map {
          case ((uid, v), goal) =>
            (uid.toFeatures(config.userDataDescriptors) ++ Seq(v.id.toDouble), goal.toDouble)
        }

        val cats =
          BitSet(
            p.config.userDataDescriptors.zipWithIndex
                                        .filter { case (d, _) => d.categorical }
                                        .map    { case (_, i) => i } :_*
          )

        (s, cats + /* variation */ config.userDataDescriptors.size)
    }


  /**
    * Returns current app-instance getStatus
    **/
  private def getStatus(from: Epoch = 0l): Future[AppInstanceStatus] = {

    import Storage.Commands.{Count, CountResponse}

    val state   = this.runState
    val storage = this.storage()

    import reactivemongo.bson.BSONDocument

    for {

      eventsAllStart <-
        ask(storage, Count[StartEvent](
          Event.`appId`     -> appId,
          Event.`type`      -> Event.`type:Start`,
          Event.`timestamp` -> BSONDocument("$gt" -> from.ts)
        )).mapTo[CountResponse]
          .map(_.count)

      eventsAllFinish <-
        ask(storage, Count[FinishEvent](
          Event.`appId`     -> appId,
          Event.`type`      -> Event.`type:Finish`,
          Event.`timestamp` -> BSONDocument("$gt" -> from.ts)
        )).mapTo[CountResponse]
          .map(x => x.count)

    } yield AppInstanceStatus(state, eventsAllStart, eventsAllFinish)
  }


  // TODO(kudinkin): move config out of predictor
  private def getConfig: AppInstanceConfig =
    predictor.collect { case p => p.config } get // getOrElse AppInstanceConfig.sentinel

  private def reloadConfig(): Future[AppInstanceConfig.Record] = {
    import Storage.Commands.{Load, LoadResponse}

    val f = { for (
      r <- ask[LoadResponse[AppInstanceConfig.Record]](this.storage(), Load[AppInstanceConfig.Record](appId))
    ) yield r.seq.collectFirst(Identity.partial()) } map {
      case Some(c)  => c
      case None     => throw new Exception(s"Failed to retrieve app's {#${appId}} configuration!")
    }

    f.andThen {
      case Success(cfg) => applyConfig(cfg)
      case Failure(t)   =>
        log.error(t, s"Failed to retrieve app's {#{}} configuration!", appId)
    }
  }

  private def applyConfig(r: AppInstanceConfig.Record): Unit = {
    updateState(r.runState) match {
      case State.Predicting(_) =>
        predictor = Some(Predictor(r.config))

      case State.Training =>
        /* NOP */

      case State.NoData   =>
        predictor = Some(Predictor.random(r.config))

      case State.Suspended =>
        predictor = None

      case _ =>
        throw new UnsupportedOperationException
    }
  }

  private def updateState(state: State): State = {
    runState = state
    runState
  }

  /**
    * Check whether it's a proper time to shutdown
    **/
  private def commitSuicide(check: Boolean = true) {
    runState is State.Suspended then {
      takePoison()
      return
    }
  }

  private def takePoison() {
    import akka.actor.PoisonPill

    self ! PoisonPill
  }

  /**
    * Switches current-state of the app-instance
    *
    * @param state target state being switched to
    */
  private def switchState(state: State): Future[AppInstanceConfig.Record] = {
    import Storage._

    ask[UpdateResponse](this.storage(), Update[AppInstanceConfig.Record](appId) {
      AppInstanceConfig.Record.`runState` -> state
    }).map      { r => r.ok }
      .andThen  {
        case Failure(t) => log.error(t, s"Failed to switch state to '${state}'!")
      }
      .flatMap  {
        case _ => reloadConfig()
      }
  }

  private def buildTrainingSample(maxSize: Int): Future[Seq[((UserIdentity, Variation), Goal#Type)]] = {
    import com.flp.control.storage.Storage

    val storage = this.storage()

    def coalesce(es: Event*): ((UserIdentity, Variation), Goal#Type) = {
      val s = es.filter { _.isInstanceOf[StartEvent] }
                .collectFirst({ case x => x.asInstanceOf[StartEvent] })

      val f = es.filter { _.isInstanceOf[FinishEvent] }
                .collectFirst({ case x => x.asInstanceOf[FinishEvent] })

      s match { case Some(e) => ((e.identity, e.variation), f.isDefined) }
    }

    { for (
      vs <- ask(storage, Storage.Commands.Load[StartEvent](Event.`appId` -> appId)(maxSize))
              .mapTo[Storage.Commands.LoadResponse[StartEvent]];

      rs <- ask(storage, Storage.Commands.Load[FinishEvent](Event.`appId` -> appId)(maxSize))
              .mapTo[Storage.Commands.LoadResponse[FinishEvent]]

    ) yield (vs.seq ++ rs.seq).groupBy(e => e.session)
                              .toSeq
                              .map { case (s, es) => coalesce(es:_*) }
    } andThen {
      case Failure(t) =>
        log.error(t, "Failed to compose training sample (#{{}})!", appId)
    }
  }


  //
  // Controlling hooks
  //

  private def stop(): Future[AppInstanceStatus] =
    switchState(State.Suspended).flatMap { _ => getStatus() }

  override def receive: Receive = trace {

    //
    // TODO(kudinkin): Transition to `akka.FSM` (!)
    //

    case Commands.ReloadConfig() => runState is Any then {
      // TODO(kudinkin): that's a hack until `record` & `appconfig` merged
      reloadConfig() map { acr => acr.config } pipeTo sender()
    }

    case Commands.StatusRequest() => runState is Any then {
      getStatus() pipeTo sender()
    }

    case Commands.ConfigRequest() => runState is Any then {
      sender ! getConfig
    }

    case Commands.PredictRequest(uid) => runState except State.Suspended then {
      // TODO(kudinkin): move?
      self    ! Commands.TrainRequest()
      sender  ! Commands.PredictResponse(predict(uid))
    }

    case Commands.TrainRequest() => runState except State.Suspended or
                                                    State.Training  then {

      assert(runState != State.Suspended || runState != State.Training)

      train() pipeTo sender()
    }

    case Commands.StopRequest() => runState is Any then {
      stop() pipeTo sender()
    }

    case Commands.Suicide() => commitSuicide()
  }


  /**
    * Reload getConfig and start self-destructing mechanic
    **/
  override def preStart(): Unit = {
    import scala.concurrent.duration._

    // TODO(kudinkin): switch to `ready` to swallow exceptions

    Await.result(
      reloadConfig().andThen {
        case Failure(t) => takePoison()
      },
      30.seconds
    )

    context.system.scheduler.scheduleOnce(2.minutes) { self ! Commands.Suicide() }
  }
}

case class AppInstanceStatus(
  runState: AppInstance.State,
  eventsAllStart    : Int,
  eventsAllFinish   : Int
)

object AppInstanceStatus {
  val empty: AppInstanceStatus =
    AppInstanceStatus(
      runState          = AppInstance.State.NoData,
      eventsAllStart    = 0,
      eventsAllFinish   = 0
    )
}

/**
  * App-instance's configuration
  *
  * @param variations available variations
  * @param userDataDescriptors  user-data descriptors converting its identity into point
  *                             in high-dimensional feature-space
  */
case class AppInstanceConfig(
  variations:           Seq[Variation],
  userDataDescriptors:  Seq[UserDataDescriptor],
  model:                Option[AppInstanceConfig.Model]
)


// TODO(kudinkin): Merge `AppInstanceConfig` & `AppInstanceConfig.Record`
object AppInstanceConfig {

  type ClassificationModel  = SparkClassificationModel[_ <: SparkModel.Model]
  type RegressionModel      = SparkRegressionModel[_ <: SparkModel.Model]

  type Model = Either[ClassificationModel, RegressionModel]

  val `variations`  = "variations"
  val `descriptors` = "descriptors"
  val `model`       = "model"

  val sentinel = AppInstanceConfig(variations = Seq(), userDataDescriptors = Seq(), model = None)

  case class Record(
    appId:    String,
    runState: AppInstance.State = AppInstance.State.NoData,
    config:   AppInstanceConfig = AppInstanceConfig.sentinel
  )

  object Record {
    val `config`    = "config"
    val `runState`  = "runState"

    def notFound(appId: String) =
      Record(appId, AppInstance.State.NoData, AppInstanceConfig.sentinel)
  }
}

trait AppInstanceMessage[Response]

trait AppInstanceAutoStartMessage[Response] extends AppInstanceMessage[Response]

object AppInstance {

  import org.apache.commons.lang3.StringUtils._
  import reactivemongo.bson._

  import scala.concurrent.duration._

  def fixId(appId: String): String = appId match {

    case "new" => BSONObjectID.generate.stringify
    case _ =>     leftPad(appId, 24, '0')
  }

  def actorName(appId: String): String = {
    s"app-$appId"
  }


  /**
    * Designates particular epoch associated with event (identified by `eid`)
    *
    * @param ts (server's) timestamp of the event associated with the epoch
    */
  implicit class Epoch(val ts: Long) {

    override def toString: String = s"{ Epoch: #${ts} }"

  }

  object Epoch {
    val anteChristum: Epoch = 0l
  }


  /****************************/
  /** STATES                  */
  /****************************/

  sealed trait State extends StateEx

  trait StateEx {
    val typeName = deduce

    private def deduce: String = {
      import scala.reflect.runtime.{universe => u}
      u.runtimeMirror(getClass.getClassLoader).moduleSymbol(getClass).name.decodedName.toString
    }
  }

  object State {

    val `name` = "name"

    def withName(name: String): State = {
      import scala.reflect.runtime.{ universe => u }
      Reflect.moduleFrom[State](u.typeOf[State.type].decls.filter(_.isModule)
                                                          .filter(_.name.decodedName.toString == name)
                                                          .head)
    }

    /**
      * Transient state designating app's pre-start phase
      */
    case object Loading extends State

    /**
      * No-data phase of the app, when collected sample isn't
      * representative enough to build relevant predictor
      */
    case object NoData extends State

    /**
      * Active phase of the app with a long enough sample
      * to have properly trained predictor
      */
    final case class Predicting(from: Epoch) extends State

    object Predicting extends StateEx {
      val `from` = "from"
    }

    /**
      * Training phase of the app when no more training requests
      * are accepted until finished
      */
    case object Training extends State

    /**
      * Suspended
      */
    case object Suspended extends State

  }

  object Commands {

    private[instance] case class Suicide()

    case class ReloadConfig() extends AppInstanceAutoStartMessage[AppInstanceConfig]

    case class StatusRequest() extends AppInstanceAutoStartMessage[AppInstanceStatus]
    case class ConfigRequest() extends AppInstanceAutoStartMessage[AppInstanceConfig]

    trait AppInstanceChangeRunStateMessage extends AppInstanceAutoStartMessage[AppInstanceStatus]

    case class StartRequest() extends AppInstanceChangeRunStateMessage
    case class StopRequest() extends AppInstanceChangeRunStateMessage
    
    val predictTimeout: Timeout = 500.milliseconds

    case class PredictRequest(identity: UserIdentity) extends AppInstanceAutoStartMessage[PredictResponse]
    case class PredictResponse(variation: Variation)

    val trainTimeout: Timeout = 30.seconds

    private[instance] case class TrainRequest() extends AppInstanceAutoStartMessage[TrainResponse]
    private[instance] case class TrainResponse(error: Double)

  }
}