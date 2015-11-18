package com.flp.control.instance

import akka.actor.ActorRef
import akka.pattern.pipe
import akka.util.Timeout
import com.flp.control.akka.ExecutingActor
import com.flp.control.boot.Boot
import com.flp.control.instance.AppInstance.Commands.{ApplyConfig, GetStatusResponse}
import com.flp.control.model._
import com.flp.control.spark.SparkDriverActor
import com.flp.control.storage.Storage
import com.flp.control.storage.Storage.Commands.UpdateResponse
import com.flp.control.util.boolean2Int

import scala.concurrent.Future

class AppInstanceActor(val appId: String) extends ExecutingActor {
  import AppInstance._

  private val sparkDriverRef = Boot.actor(classOf[SparkDriverActor].getName)

  private var runState:   AppInstanceRunState.Value = AppInstanceRunState.Loading
  private var predictor:  Option[Predictor] = None

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

    import SparkDriverActor.Commands.{TrainRegressorResponse, TrainRegressor}

    val SAMPLE_MAX_SIZE = 1000

    status().flatMap {
      case s =>

        // TODO(kudinkin): Gate the whole process through `applyConfig`

        if (shouldTrain(s))
          buildTrainingSample(SAMPLE_MAX_SIZE)
            .flatMap  { sample => ask[TrainRegressorResponse](sparkDriverRef, TrainRegressor(explain(sample))) }
            .map      {
              case TrainRegressorResponse(model, error) =>
                predictor = Some(buildPredictor(config(), model))
                Some(error)
            }
        else
          Future { None }
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

      s.get match { case e => ((e.identity, Variation.sentinel), f.isDefined) }
    }

    // _DBG

//    ask(storage, Storage.Commands.Load[StartEvent](Event.`appId` -> appId)(maxSize))
//      .mapTo[Storage.Commands.LoadResponse[StartEvent]]
//      .map { case s => println("XOXO: " + s); }
//
//    ask(storage, Storage.Commands.Load[FinishEvent](Event.`appId` -> appId)(maxSize))
//      .mapTo[Storage.Commands.LoadResponse[FinishEvent]]
//      .map { case s => println("XYXY: " + s); }

    val f = for (
      vs <- ask(storage, Storage.Commands.Load[StartEvent](Event.`appId` -> appId)(maxSize))
              .mapTo[Storage.Commands.LoadResponse[StartEvent]];

      rs <- ask(storage, Storage.Commands.Load[FinishEvent](Event.`appId` -> appId)(maxSize))
              .mapTo[Storage.Commands.LoadResponse[FinishEvent]]

    ) yield (vs.seq ++ rs.seq).groupBy(e => e.session)
                              .toSeq
                              .map { case (s, es) => coalesce(es:_*) }

//    f.onFailure { case t => t.printStackTrace }
//    f.onComplete { case x => println(x.get) }

    f.map { seq => println(seq); seq }
  }

  override def receive: Receive = trace {
    case Commands.ApplyConfig(cfg)    => sender ! applyConfig(cfg)
    case Commands.GetStatusRequest()  => statusAsResponse() pipeTo sender()
    case Commands.GetConfigRequest()  => sender ! Commands.GetConfigResponse(config())

    case Commands.PredictRequest(uid) => sender ! Commands.PredictResponse(predict(uid))
    case Commands.TrainRequest()      => train() pipeTo sender()

    case Commands.SelfKillCheck() => tryShutdown()
    case Commands.StartRequest()  => changeRunStateToStart() pipeTo sender()
    case Commands.StopRequest()   => changeRunStateToStop() pipeTo sender()

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
  runState: AppInstanceRunState.Value = AppInstanceRunState.NoData,
  eventsAllStart    : Int = 0,
  eventsAllFinish   : Int = 0,
  eventsLearnStart  : Int = 0,
  eventsLearnFinish : Int = 0
)

object AppInstanceStatus {
  val empty: AppInstanceStatus = AppInstanceStatus()
}

case class AppInstanceConfig(variations: Seq[Variation], userDataDescriptors: Seq[UserDataDescriptor])

// TODO(kudinkin): Merge `AppInstanceConfig` & `AppInstanceConfig.Record`
object AppInstanceConfig {
  val empty = AppInstanceConfig(Seq(), Seq())

  val `variations`  = "variations"
  val `descriptors` = "descriptors"

  case class Record(
    appId:    String,
    runState: AppInstanceRunState.Value = AppInstanceRunState.Stopped,
    config:   AppInstanceConfig         = AppInstanceConfig.empty
  )

  object Record {
    val `config`    = "config"
    val `runState`  = "runState"

    def notFound(appId: String) =
      Record(appId, AppInstanceRunState.NoData, AppInstanceConfig.empty)
  }
}

trait AppInstanceMessage[Response]

trait AppInstanceAutoStartMessage[Response] extends AppInstanceMessage[Response]

trait AppInstanceChangeRunStateMessage extends AppInstanceAutoStartMessage[GetStatusResponse]

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

  object Commands {

    private[instance] case class SelfKillCheck()
    private[instance] case class ApplyConfig(config: AppInstanceConfig.Record)

    case class GetStatusRequest() extends AppInstanceAutoStartMessage[GetStatusResponse]
    case class GetStatusResponse(status: AppInstanceStatus)

    case class GetConfigRequest() extends AppInstanceAutoStartMessage[GetConfigResponse]
    case class GetConfigResponse(config: AppInstanceConfig)

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