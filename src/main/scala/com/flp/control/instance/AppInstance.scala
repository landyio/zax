package com.flp.control.instance

import akka.actor.ActorRef
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.flp.control.akka.DefaultActor
import com.flp.control.boot.Boot
import com.flp.control.instance.AppInstance.Commands.{ApplyConfig, GetStatusResponse}
import com.flp.control.model._
import com.flp.control.storage.Storage
import com.flp.control.storage.Storage.Commands.UpdateResponse

import scala.concurrent.Future

class AppInstanceActor(val appId: String) extends DefaultActor {
  import AppInstance._

  private var runState: AppInstanceRunState.Value = AppInstanceRunState.Loading
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

  /**
    * Returns current app-instance status
    **/
  private def status(): Future[AppInstanceStatus] = {

    import com.flp.control.storage.Storage

    val state = this.runState

    // TODO(kudinkin): Remove this
    val fake: Boolean = state match {
      case AppInstanceRunState.NoData => true
      case _ => false
    }
    if (fake) {
      // its fake status, no db lookup required
      return Future { AppInstanceStatus(state) }
    }

    val storage = this.storage()

    // construct real status
    for {

      // all started events
      eventsAllStart <-
        ask(storage, Storage.Commands.Count[StartEvent](
          Event.`appId` -> appId,
          Event.`type`  -> Event.`type:Start`
        )).mapTo[Storage.Commands.CountResponse]
          .map(_.count)

      // all finished events
      eventsAllFinish <-
        ask(storage, Storage.Commands.Count[StartEvent](
          Event.`appId` -> appId,
          Event.`type`  -> Event.`type:Finish`
        )).mapTo[Storage.Commands.CountResponse]
          .map(x => x.count)

    } yield AppInstanceStatus(state, eventsAllStart, eventsAllFinish)
  }

  /** @return status wrapped into {{{GetStatusResponse}}} */
  private def statusAsResponse(): Future[GetStatusResponse] = status().map { s => Commands.GetStatusResponse(s) }

  /** @return config (variants) from {{{predictor}}} */
  private def config(): AppInstanceConfig = AppInstanceConfig(
    variations = predictor.map(_.variations.values.toSeq) getOrElse Seq()
  )

  /** @return {{{ Future { "load config from mongo, then apply it" } }}} */
  private def reloadConfig(): Future[ApplyConfig] = {

    val storage = this.storage()
    val f =
      (storage ? Storage.Commands.Load[AppInstanceConfig.Record](appId))
        .map { r => r.asInstanceOf[Storage.Commands.LoadResponse[AppInstanceConfig.Record]] }
        .map { r => r.seq.collectFirst({ case x => x }) }
        .map { o => o.getOrElse(AppInstanceConfig.Record.notFound(appId)) }
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

  private def applyConfig(cfg: AppInstanceConfig.Record): Unit = {
    // TODO: read from cfg / cfg.config
    // TODO: check for model, modify cfg.runState

    //
    // TODO(kudinkin): Schema?
    //

    val userdata: Seq[UserDataDescriptor] = Seq(
      UserDataDescriptor("browser"),
      UserDataDescriptor("os")
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
  private def buildPredictor(config: AppInstanceConfig, userDataDescriptors: Seq[UserDataDescriptor]) =
    Predictor(
      config.variations.zipWithIndex.map { case (el, idx) => idx -> el }.toMap,
      userDataDescriptors
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

    val request: Storage.Commands.UpdateRequest[AppInstanceConfig.Record] = Storage.Commands.Update[AppInstanceConfig.Record](appId) {
      AppInstanceConfig.Record.`runState` -> targetRunState.toString
    }

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
    case Commands.ApplyConfig(cfg)    => sender ! applyConfig(cfg)
    case Commands.GetStatusRequest()  => statusAsResponse() pipeTo sender()
    case Commands.GetConfigRequest()  => sender ! Commands.GetConfigResponse(config())
    case Commands.PredictRequest(uid) => sender ! Commands.PredictResponse(predict(uid))

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

case class AppInstanceConfig(variations: Seq[Variation])

// TODO(kudinkin): Merge `AppInstanceConfig` & `AppInstanceConfig.Record`
object AppInstanceConfig {
  val empty = AppInstanceConfig(Seq())

  case class Record(
    appId: String,
    runState: AppInstanceRunState.Value = AppInstanceRunState.Stopped,
    config: AppInstanceConfig = AppInstanceConfig.empty
  )

  object Record {
    val `config`    = "config"
    val `runState`  = "runState"

    def notFound(appId: String) =
      Record(appId, AppInstanceRunState.NoData, AppInstanceConfig.empty)
  }
}

trait AppInstanceMessage[Response] {
}

trait AppInstanceAutoStartMessage[Response] extends AppInstanceMessage[Response] {
}

trait AppInstanceChangeRunStateMessage extends AppInstanceAutoStartMessage[GetStatusResponse] {
}

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

  }
}