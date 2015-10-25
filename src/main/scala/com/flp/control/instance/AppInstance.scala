package com.flp.control.instance

import akka.actor.ActorRef
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.flp.control.akka.DefaultActor
import com.flp.control.boot.Boot
import com.flp.control.instance.AppInstance.Commands.{ApplyConfig, GetStatusResponse}
import com.flp.control.model._
import com.flp.control.storage.Storage.Commands.UpdateResponse

import scala.concurrent.Future

class AppInstanceActor(val appId: String) extends DefaultActor {
  import AppInstance._

  private var runState: AppInstanceRunState.Value = AppInstanceRunState.Loading
  private var predictor: Option[AppPredictor] = None

  /** @return storage ref */
  private def storage(): ActorRef = Boot.actor(com.flp.control.storage.Storage.actorName)

  /** @return prediction result (call predictor with specified {{{identity}}}) */
  private def predict(identity: IdentityData.Type): Variation.Type = predictor
    .map { p => p.predict(identity) }
    .getOrElse { Variation( Map() ) }

  /** @return current status */
  private def status(): Future[AppInstanceStatus] = {

    val runState = this.runState
    val fake: Boolean = runState match {
      case AppInstanceRunState.NoData => true
      case _ => false
    }
    if (fake) {
      // its fake status, no db lookup required
      return for { s <- Future { runState } } yield AppInstanceStatus( runState = s )
    }

    import com.flp.control.storage.Storage
    val storage = this.storage()

    // construct real status
    for {

      // run state (on function call moment)
      s <- Future { runState }

      // all started events
      eventsAllStart <- ask(storage, Storage.Commands.Count[StartEvent](
        Event.`appId` -> appId,
        Event.`type` -> Event.`type:Start`
      )).mapTo[Storage.Commands.CountResponse].map(x => x.count)

      // all finished events
      eventsAllFinish <- ask(storage, Storage.Commands.Count[StartEvent](
        Event.`appId` -> appId,
        Event.`type` -> Event.`type:Finish`
      )).mapTo[Storage.Commands.CountResponse].map(x => x.count)

    } yield AppInstanceStatus(
        runState = s,
        eventsAllStart = eventsAllStart,
        eventsAllFinish = eventsAllFinish
    )
  }

  /** @return status wrapped into {{{GetStatusResponse}}} */
  private def statusAsResponse(): Future[GetStatusResponse] = status().map { s => Commands.GetStatusResponse(s) }

  /** @return config (variants) from {{{predictor}}} */
  private def config(): AppInstanceConfig = AppInstanceConfig(
    variants = predictor.map( p => p.params.map { case (k,v) => k -> v.variations.values.toSeq } ).getOrElse( Map() )
  )

  /** @return {{{ Future { "load config from mongo, then apply it" } }}} */
  private def reloadConfig(): Future[ApplyConfig] = {
    import com.flp.control.storage.Storage
    val storage: ActorRef = this.storage()
    val f: Future[ApplyConfig] = (storage ? Storage.Commands.Load[AppInstanceConfigRecord](appId))
      .map { res => res.asInstanceOf[Storage.Commands.LoadResponse[AppInstanceConfigRecord]].obj }
      .map { opt => opt.getOrElse(AppInstanceConfigRecord.notFound(appId)) }
      .map { cfg => Commands.ApplyConfig(cfg) }

    f.flatMap { req => (self ? req).map { x => req } }
  }

  /** @return {{{ Future { "load config from mongo, apply it, then ask self for the status" } }}} */
  private def reloadConfigAndGetStatus(): Future[GetStatusResponse] = {
    reloadConfig().flatMap {
      s => self.ask(Commands.GetStatusRequest()).map {
        r => r.asInstanceOf[Commands.GetStatusResponse]
      }
    }
  }

  /** @return true, */
  private def applyConfig(cfg: AppInstanceConfigRecord): Unit = {
    // TODO: read from cfg / cfg.config
    // TODO: check for model, modify cfg.runState

    // TODO(kudinkin): Schema?

    val userdata: Seq[UserDataDescriptor] = Seq(
      UserDataDescriptor("browser")
    )
    runState = cfg.runState // TODO: modify me
    predictor = runState match {
      case AppInstanceRunState.Training   => Some(buildPredictor(cfg.config, userdata))
      case AppInstanceRunState.Prediction => Some(buildPredictor(cfg.config, userdata))
      case _ => None
    }
  }

  /**
    * Predictor builder
    *
    * @return new {{{AppPredictor}}} for specified {{{config}}} and {{{userDataDescriptors}}} */
  private def buildPredictor(config: AppInstanceConfig, userDataDescriptors: Seq[UserDataDescriptor]): AppPredictor = AppPredictor(
    config.variants.map {
      case (k, v) => k -> AppParamPredictor(v.zipWithIndex.map { case (el, idx) => idx -> el }.toMap, userDataDescriptors)
    }
  )

  /** check self to be killed, schedule next {{{selfKillCheck}}} call */
  private def selfKillCheck(check: Boolean = true): Unit = {
    if (check) {
      val kill = runState match {
        case AppInstanceRunState.NoData => true
        case AppInstanceRunState.Stopped => true
        case _ => false
      }
      if (kill) {
        import akka.actor.PoisonPill
        self ! PoisonPill
        return
      }
    }
    import scala.concurrent.duration._
    context.system.scheduler.scheduleOnce(2.minutes) { self ! Commands.SelfKillCheck() }
  }

  /** @return {{{ Future { "update `runState` to specified `targetRunState`" } }}}*/
  private def doUpdateRunStat(targetRunState: AppInstanceRunState.Value): Future[Boolean] = {
    import com.flp.control.storage.Storage

    val request: Storage.Commands.UpdateRequest[AppInstanceConfigRecord] = Storage.Commands.Update.id[AppInstanceConfigRecord](appId) {
      AppInstanceConfigRecord.`runState` -> targetRunState.toString
    } (
      asking = true
    )

    val storage = this.storage()
    val future: Future[Boolean] = (storage ? request).map {
      x => x.asInstanceOf[UpdateResponse].ok
    }

    future
  }

  /** @return {{{ Future{ "set `runState` = AppInstanceRunState.Training" } }}}*/
  private def changeRunStateToStart(): Future[GetStatusResponse] = {
    doUpdateRunStat(AppInstanceRunState.Training).flatMap {
      x => reloadConfigAndGetStatus()
    }
  }

  /** @return {{{ Future{ "set `runState` = AppInstanceRunState.Stopped" } }}}*/
  private def changeRunStateToStop(): Future[GetStatusResponse] = {
    doUpdateRunStat(AppInstanceRunState.Stopped).flatMap {
      x => reloadConfigAndGetStatus()
    }
  }

  /** akka callback */
  override def receive: Receive = trace {
    case Commands.ApplyConfig(cfg)        => sender ! applyConfig(cfg)
    case Commands.GetStatusRequest()      => statusAsResponse() pipeTo sender()
    case Commands.GetConfigRequest()      => sender ! Commands.GetConfigResponse(config())
    case Commands.PredictRequest(params)  => sender ! Commands.PredictResponse(predict(params))

    case Commands.SelfKillCheck() => selfKillCheck()
    case Commands.StartRequest()  => changeRunStateToStart() pipeTo sender()
    case Commands.StopRequest()   => changeRunStateToStop() pipeTo sender()

  }

  /** akka prestart - reload config & start selfKill */
  override def preStart(): Unit = {
    reloadConfig()
    selfKillCheck(false)
  }

  //override def postStop(): Unit = {}
}

object AppInstanceRunState extends Enumeration {
  val Loading     = Value // transient state
  val NoData      = Value // finish state
  val Stopped     = Value
  val Training    = Value
  val Prediction  = Value
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

case class AppInstanceConfig(
  variants: Map[String, Seq[String]]
)

object AppInstanceConfig {
  val empty: AppInstanceConfig = AppInstanceConfig( Map() )
}

trait AppInstanceMessage[Response] {
}

trait AppInstanceAutoStartMessage[Response] extends AppInstanceMessage[Response] {
}

trait AppInstanceChangeRunStateMessage extends AppInstanceAutoStartMessage[GetStatusResponse] {
}

object AppInstance {
  import scala.concurrent.duration._

  import reactivemongo.bson._
  import org.apache.commons.lang3.StringUtils._

  def fixId(appId: String): String = appId match {

    case "new" => BSONObjectID.generate.stringify
    case _ =>     leftPad(appId, 24, '0')
  }

  def actorName(appId: String): String = {
    s"app-$appId"
  }

  object Commands {

    private[instance] case class SelfKillCheck()
    private[instance] case class ApplyConfig(config: AppInstanceConfigRecord)

    case class GetStatusRequest() extends AppInstanceAutoStartMessage[GetStatusResponse]
    case class GetStatusResponse(status: AppInstanceStatus)

    case class GetConfigRequest() extends AppInstanceAutoStartMessage[GetConfigResponse]
    case class GetConfigResponse(config: AppInstanceConfig)

    case class StartRequest() extends AppInstanceChangeRunStateMessage
    case class StopRequest() extends AppInstanceChangeRunStateMessage
    
    val predictTimeout: Timeout = 500.milliseconds

    case class PredictRequest(identity: IdentityData.Type) extends AppInstanceAutoStartMessage[PredictResponse]
    case class PredictResponse(variation: Variation.Type)

  }
}