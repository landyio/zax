package com.flp.control.instance

import akka.actor.ActorRef
import akka.pattern.pipe
import akka.util.Timeout
import com.flp.control.akka.ExecutingActor
import com.flp.control.boot.Boot
import com.flp.control.model._
import com.flp.control.spark.SparkDriverActor
import com.flp.control.storage.Storage
import com.flp.control.storage.Storage.Commands.UpdateResponse
import com.flp.control.util.boolean2Int

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

import scala.language.{implicitConversions, postfixOps}

class AppInstanceActor(val appId: String) extends ExecutingActor {

  import AppInstance._

  import util.State._

  private val sparkDriverRef = Boot.actor(classOf[SparkDriverActor].getName)

  private var runState: State.Value = State.Loading
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
  private def predict(identity: UserIdentity): Variation =
    predictor
      .map        { p => p.predictFor(identity) }
      .getOrElse  { Variation.sentinel }

  private def shouldTrain(s: AppInstanceStatus): Boolean = s.eventsAllFinish >= 25

  /**
    * Retrains specified app
    *
    * @return
    */
  private def train(): Future[Option[Double]] = {

    import SparkDriverActor.Commands.{TrainRegressor, TrainRegressorResponse}

    val SAMPLE_MAX_SIZE = 1000

    getStatus.flatMap {
      case s =>

        if (shouldTrain(s)) {

          buildTrainingSample(SAMPLE_MAX_SIZE)
            .flatMap { sample => ask[TrainRegressorResponse](sparkDriverRef, TrainRegressor(explain(sample))) }
            .flatMap {
              case TrainRegressorResponse(model, error) =>

                import Storage.{appInstanceConfigBSON, appInstanceConfigRecordBSON}

                ask[UpdateResponse](this.storage(), Update[AppInstanceConfig.Record](appId) {
                  AppInstanceConfig.Record.`config` ->
                    getConfig.copy(model =
                      Some(
                        Right[AppInstanceConfig.ClassificationModel, AppInstanceConfig.RegressionModel](
                          model.asInstanceOf[AppInstanceConfig.RegressionModel]
                        )
                      )
                    )
                })

                switchState(State.Predicting).map(
                  _ => Some(error)
                )
            }
            .andThen {
              case Failure(t) =>
                log.error(t, "Training of predictor for #{{}} failed!", appId)
            }
        } else {
          Future { None }
        }
    }
  }

  // TODO(kudinkin): Move

  private def explain(sample: Seq[((UserIdentity, Variation), Goal#Type)]): Seq[(Seq[Double], Double)] = {
    predictor match {
      case Some(p) =>
        sample.map {
          case ((uid, v), goal) => (uid.toFeatures(p.userDataDescriptors) ++ Seq(v.id.toDouble), goal.toDouble)
        }
    }
  }


  /**
    * Returns current app-instance getStatus
    **/
  private def getStatus: Future[AppInstanceStatus] = {

    import com.flp.control.storage.Storage

    val state = this.runState

    //
    // TODO(kudinkin): Remove this
    //
    val fake: Boolean = state match {
      case State.NoData => true
      case _ => false
    }
    if (fake) {
      // its fake getStatus, no db lookup required
      return Future {
        AppInstanceStatus(state)
      }
    }

    val storage = this.storage()

    for {

      eventsAllStart <-
      ask(storage, Storage.Commands.Count[StartEvent](
        Event.`appId` -> appId,
        Event.`type` -> Event.`type:Start`
      )).mapTo[Storage.Commands.CountResponse]
        .map(_.count)

      eventsAllFinish <-
      ask(storage, Storage.Commands.Count[StartEvent](
        Event.`appId` -> appId,
        Event.`type` -> Event.`type:Finish`
      )).mapTo[Storage.Commands.CountResponse]
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
      case State.Predicting =>
        predictor = Some(Predictor(r.config))

      case State.NoData =>
        predictor = Some(Predictor.random(r.config))

      case State.Suspended =>
        predictor = None

      case _ =>
        throw new UnsupportedOperationException
    }
  }

  private def updateState(state: State.Value): State.Value = {
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
  private def switchState(state: State.Value): Future[AppInstanceConfig.Record] = {
    ask[UpdateResponse](this.storage(), Update[AppInstanceConfig.Record](appId) {
      AppInstanceConfig.Record.`runState` -> state.toString
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
      println("XOXO: ", es)

      val s = es.filter { _.isInstanceOf[StartEvent] }
                .collectFirst({ case x => x.asInstanceOf[StartEvent] })

      val f = es.filter { _.isInstanceOf[FinishEvent] }
                .collectFirst({ case x => x.asInstanceOf[FinishEvent] })

      s match { case Some(e) => ((e.identity, Variation.sentinel), f.isDefined) }
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
    switchState(State.Suspended).flatMap { _ => getStatus }

  override def receive: Receive = trace {

    //
    // TODO(kudinkin): Transition to `akka.FSM` (!)
    //

    case Commands.ReloadConfig() => runState is Any then {
      // TODO(kudinkin): that's a hack until `record` & `appconfig` merged
      reloadConfig() map { acr => acr.config } pipeTo sender()
    }

    case Commands.StatusRequest() => runState is Any then {
      getStatus pipeTo sender()
    }

    case Commands.ConfigRequest() => runState is Any then {
      sender ! getConfig
    }

    case Commands.PredictRequest(uid) => runState is  State.NoData      or
                                                      State.Predicting  then {
      sender ! Commands.PredictResponse(predict(uid))
    }

    case Commands.TrainRequest() => runState is State.NoData      or
                                                State.Predicting  then {
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
  runState: AppInstance.State.Value = AppInstance.State.NoData,
  eventsAllStart    : Int = 0,
  eventsAllFinish   : Int = 0,
  eventsLearnStart  : Int = 0,
  eventsLearnFinish : Int = 0
)

object AppInstanceStatus {
  val empty: AppInstanceStatus = AppInstanceStatus()
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
  model:                AppInstanceConfig.Model
)


// TODO(kudinkin): Merge `AppInstanceConfig` & `AppInstanceConfig.Record`
object AppInstanceConfig {

  type ClassificationModel  = SparkClassificationModel[_ <: SparkModel.Model]
  type RegressionModel      = SparkRegressionModel[_ <: SparkModel.Model]

  type Model = Option[Either[ClassificationModel, RegressionModel]]

  val `variations`  = "variations"
  val `descriptors` = "descriptors"
  val `model`       = "model"

  val sentinel = AppInstanceConfig(variations = Seq(), userDataDescriptors = Seq(), model = None)

  case class Record(
    appId:    String,
    runState: AppInstance.State.Value = AppInstance.State.NoData,
    config:   AppInstanceConfig       = AppInstanceConfig.sentinel
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


  object State extends Enumeration {

    /**
      * Transient state designating app's pre-start phase
      */
    val Loading = Value

    /**
      * No-data phase of the app, when collected sample isn't
      * representative enough to build relevant predictor
      */
    val NoData = Value

    /**
      * Active phase of the app with a long enough sample
      * to have properly trained predictor
      */
    val Predicting = Value

    /**
      * Suspended
      */
    val Suspended = Value
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

    case class TrainRequest() extends AppInstanceAutoStartMessage[TrainResponse]
    case class TrainResponse(error: Double)

  }
}