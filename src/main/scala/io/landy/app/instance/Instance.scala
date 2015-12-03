package io.landy.app.instance

import akka.actor.ActorRef
import akka.pattern.pipe
import akka.util.Timeout
import io.landy.app.App
import io.landy.app.actors.ExecutingActor
import io.landy.app.driver.SparkDriverActor
import io.landy.app.instance.Instance.State.Suspended
import io.landy.app.model._
import io.landy.app.storage.Storage
import io.landy.app.storage.Storage.Commands.{Update, UpdateResponse}
import io.landy.app.util.{Identity, Reflect, boolean2Int}

import scala.collection.BitSet
import scala.compat.Platform
import scala.concurrent.{Await, Future}
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success}

class InstanceActor(val appId: Instance.Id) extends ExecutingActor {

  import Instance._
  import util.State._

  private val sparkDriverRef = App.actor(classOf[SparkDriverActor].getName)

  private var runState: State = State.Loading
  private var predictor: Option[Predictor] = None

  /**
    * Storage
    **/
  private def storage(): ActorRef = App.actor(Storage.actorName)

  /**
    * Predicts 'most-probable' variation
    *
    * @return prediction result (given user-{{{identity}}})
    **/
  private def predict(identity: UserIdentity): Variation = {
    val v = predictor
              .map { p => p.predictFor(identity) }
              .get

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
    runState match {
      case State.Predicting(from) =>
        getStatus(from.ts).map { isEligible }

      case State.NoData =>
        getStatus().map { isEligible }

      case _ => Future { false }
    }
  }

  private def isEligible(s: Status): Boolean = {

    //
    // TODO(kudinkin): We need some policy over here
    //

    val MINIMAL_POSITIVE_OUTCOMES_NO  = 1   /* 25 */
    val MINIMAL_SAMPLE_SIZE           = 10  /* 100 */

    val eligible = s.eventsAllFinish >= MINIMAL_POSITIVE_OUTCOMES_NO && s.eventsAllStart >= MINIMAL_SAMPLE_SIZE

    if (!eligible)
      log.debug("Instance {{}} isn't eligible for training right now! Status: {{}}", appId, s)

    eligible
  }

  /**
    * Retrains specified app
    */
  private def trainIfEligible(): Future[Option[Double]] = {

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

                import Storage.Persisters.{instanceConfigPersister, instanceRecordPersister}

                ask[UpdateResponse](this.storage(), Update[Instance.Record](appId) {
                  Instance.Record.`config` ->
                    getConfig.copy(model = Some(Right(model.asInstanceOf[Instance.Config.RegressionModel])))
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

  /**
    * Explains given sample from the language of the app's model down to the language
    * of the trainer: 'squeezing' features down to corresponding numerical values
    */
  private def explain(sample: Seq[((UserIdentity, Variation.Id), Goal#Type)]): (Seq[(Seq[Double], Double)], BitSet) =
    predictor match {
      case Some(p) =>

        // Map variations into sequential indices
        val mapped = getConfig.variations .zipWithIndex
                                          .map { case (v, idx) => v.id -> idx }
                                          .toMap

        // Convert sample into user-data-descriptors into numerical 'features'
        val s = sample.collect {
          case ((uid, v), goal) if mapped.contains(v) =>
            (uid.toFeatures(p.config.userDataDescriptors) ++ Seq(mapped(v).toDouble), goal.toDouble)
        }

        // Designate peculiar features as categorical ones
        val cats =
          BitSet(
            p.config.userDataDescriptors.zipWithIndex
                                        .filter { case (d, _) => d.categorical }
                                        .map    { case (_, i) => i } :_*
          )

        (s, cats + p.config.userDataDescriptors.size /* variation is a category itself */)
    }


  /**
    * Returns current app-instance getStatus
    **/
  private def getStatus(from: Epoch = 0l): Future[Instance.Status] = {

    import Storage.Commands.{Count, CountResponse}
    import Storage.Persisters._

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

    } yield Instance.Status(state, eventsAllStart, eventsAllFinish)
  }


  // TODO(kudinkin): move config out of predictor
  private def getConfig: Instance.Config =
    predictor.collect { case p => p.config } get // getOrElse Instance.Config.sentinel

  private def reloadConfig(): Future[Instance.Record] = {
    import Storage.Commands.{Load, LoadResponse}
    import Storage.Persisters._

    val f = { for (
      r <- ask[LoadResponse[Instance.Record]](this.storage(), Load[Instance.Record](appId))
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

  private def applyConfig(r: Instance.Record): Unit = {
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
  private def switchState(state: State): Future[Instance.Record] = {
    import Storage.Persisters._

    ask[UpdateResponse](this.storage(), Update[Instance.Record](appId) {
      Instance.Record.`runState` -> state
    }).map      { r => r.ok }
      .andThen  {
        case Failure(t) => log.error(t, s"Failed to switch state to '{}'!", state)
      }
      .flatMap  {
        case _ => reloadConfig()
      }
  }

  private def buildTrainingSample(maxSize: Int): Future[Seq[((UserIdentity, Variation.Id), Goal#Type)]] = {
    import Storage.Commands.{Load, LoadResponse}
    import Storage.Persisters._

    val storage = this.storage()

    def coalesce(es: Event*): Option[((UserIdentity, Variation.Id), Goal#Type)] = {
      val s = es.filter { _.isInstanceOf[StartEvent] }
                .collectFirst({ case x => x.asInstanceOf[StartEvent] })

      val f = es.filter { _.isInstanceOf[FinishEvent] }
                .collectFirst({ case x => x.asInstanceOf[FinishEvent] })

      s map { case e => ((e.identity, e.variation), f.isDefined) }
    }

    { for (
      vs <- ask(storage, Load[StartEvent](Event.`appId` -> appId)(maxSize))
              .mapTo[LoadResponse[StartEvent]];

      rs <- ask(storage, Load[FinishEvent](Event.`appId` -> appId)(maxSize))
              .mapTo[LoadResponse[FinishEvent]]

    ) yield (vs.seq ++ rs.seq).groupBy(e => e.session)
                              .toSeq
                              .map    { case (s, es) => coalesce(es:_*) }
                              .filter { _.isDefined }
                              .map    { _.get }
    } andThen {
      case Failure(t) =>
        log.error(t, "Failed to compose training sample (#{{}})!", appId)
    }
  }


  //
  // Controlling hooks
  //

  // TODO(kudinkin): reinforce proper reincarnated state

  private def start(): Future[Instance.Status] =
    switchState(State.NoData).flatMap { _ => getStatus() }

  private def stop(): Future[Instance.Status] =
    switchState(State.Suspended).flatMap { _ => getStatus() }

  override def receive: Receive = trace {

    //
    // TODO(kudinkin): Transition to `akka.FSM` (!)
    //

    case Commands.ReloadConfig() => runState is Any then {
      // TODO(kudinkin): that's a hack until `record` & `appconfig` merged
      reloadConfig() map { r => r.config } pipeTo sender()
    }

    case Commands.StatusRequest() => runState is Any then {
      getStatus() pipeTo sender()
    }

    case Commands.ConfigRequest() => runState is Any then {
      sender ! getConfig
    }

    case r @ Storage.Commands.StoreRequest(_, _) => runState except State.Suspended then {
      storage() ? r pipeTo sender()
    }

    case Commands.PredictRequest(uid) => runState except State.Suspended then {
      // TODO(kudinkin): move?
      self    ! Commands.TrainRequest()
      sender  ! Commands.PredictResponse(predict(uid), runState)
    }

    case Commands.TrainRequest() => runState except State.Suspended or
                                                    State.Training  then {

      assert(runState != State.Suspended || runState != State.Training)

      trainIfEligible() pipeTo sender()
    }

    case Commands.StartRequest() => runState is Suspended then {
      start() pipeTo sender()
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


object Instance {

  import org.apache.commons.lang3.StringUtils._
  import reactivemongo.bson._

  import scala.concurrent.duration._

  /**
    * NOTA BENE
    * That's here primarily to hedge implicit conversions of the `String` to `BSONString`
    */
  case class Id(value: String)

  def actorName(appId: Instance.Id): String = {
    s"app-${appId.value}"
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


  /**
    * Instance's configuration
    *
    * @param variations available variations
    * @param userDataDescriptors  user-data descriptors converting its identity into point
    *                             in high-dimensional feature-space
    */
  case class Config(
    variations:           Seq[Variation],
    userDataDescriptors:  Seq[UserDataDescriptor],
    model:                Option[Instance.Config.Model]
  )

  object Config {

    type ClassificationModel  = SparkClassificationModel[_ <: SparkModel.Model]
    type RegressionModel      = SparkRegressionModel[_ <: SparkModel.Model]

    type Model = Either[ClassificationModel, RegressionModel]

    val `variations`  = "variations"
    val `descriptors` = "descriptors"
    val `model`       = "model"

    val empty = Instance.Config(variations = Seq(), userDataDescriptors = Seq(), model = None)
  }


  /**
    * Instance's state
    */
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
      import scala.reflect.runtime.{universe => u}
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


  /**
    * Instance's status (reports)
    *
    * @deprecated
    */
  case class Status(
    runState: Instance.State,
    eventsAllStart    : Int,
    eventsAllFinish   : Int
  )

  object Status {
    val empty: Instance.Status =
      Instance.Status(
        runState          = Instance.State.NoData,
        eventsAllStart    = 0,
        eventsAllFinish   = 0
      )
  }


  /**
    * Instance's serialized representation
    *
    * @param appId instance unique id
    * @param runState current state of the running instance
    * @param config configuration of the instance
    */
  case class Record(
    appId:    Instance.Id,
    runState: Instance.State  = Instance.State.NoData,
    config:   Instance.Config = Instance.Config.empty
  )

  object Record {
    val `config`    = "config"
    val `runState`  = "runState"

    def notFound(appId: Instance.Id) =
      Record(appId, Instance.State.NoData, Instance.Config.empty)
  }


  trait Message[Response]
  trait AutoStartMessage[Response] extends Message[Response]

  /**
    * Commands accepted by instance
    */
  object Commands {

    private[instance] case class Suicide()

    case class ReloadConfig() extends AutoStartMessage[Instance.Config]

    case class StatusRequest() extends AutoStartMessage[Instance.Status]
    case class ConfigRequest() extends AutoStartMessage[Instance.Config]

    trait AppInstanceChangeRunStateMessage extends AutoStartMessage[Instance.Status]

    case class StartRequest() extends AppInstanceChangeRunStateMessage
    case class StopRequest() extends AppInstanceChangeRunStateMessage
    
    val predictTimeout: Timeout = 500.milliseconds

    case class PredictRequest(identity: UserIdentity) extends AutoStartMessage[PredictResponse]
    case class PredictResponse(variation: Variation, state: Instance.State)

    val trainTimeout: Timeout = 30.seconds

    private[instance] case class TrainRequest() extends AutoStartMessage[TrainResponse]
    private[instance] case class TrainResponse(error: Double)

  }
}