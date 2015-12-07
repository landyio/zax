package io.landy.app.endpoints

import akka.actor._
import akka.util.Timeout
import io.landy.app.actors.{Logging, AskSupport, DefaultTimeout}
import io.landy.app.instance._
import io.landy.app.model._
import io.landy.app.endpoints.serialization.JsonSupport
import io.landy.app.storage.Storage
import io.landy.app.App
import spray.http.HttpHeaders.RawHeader
import spray.http.MediaTypes._
import spray.http._
import spray.json._
import spray.routing._
import spray.routing.authentication._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class PrivateEndpointActor extends HttpServiceActor with PrivateEndpoint
                                                    with ActorLogging {
  def receive = runRoute(route)
}

class PublicEndpointActor extends HttpServiceActor  with PublicEndpoint
                                                    with ActorLogging {
  def receive = runRoute(route)
}


trait PrivateEndpoint extends AppEndpoint {

  import Storage.Persisters._

  @inline
  private[endpoints] def control(appId: Instance.Id, principal: Principal): Route = pathPrefix("control") {
    placeholder ~
      (path("config") & `json/get`) {
        extract(ctx => ctx) { ctx => {
          import Instance.Commands.ConfigRequest

          complete(
            askApp[Instance.Config](appId, ConfigRequest())
              .map { v => v.toJson.asJsObject }
          )
        }}
      } ~
      (path("status") & `json/get`) {
        extract(ctx => ctx) { ctx => {
          import Instance.Commands.StatusRequest

          complete(
            askApp[Instance.Status](appId, StatusRequest())
              .map { v => v.toJson.asJsObject }
          )
        }}
      } ~
      (path("create") & `json/post`) {
        entity(as[JsObject]) { json => {
          val config = json.convertTo[Instance.Config]

          import Instance.Commands.ReloadConfig

          import Storage.Commands.{Store, StoreResponse}
          import Storage.Persisters.instanceRecordPersister

          complete(
            ask[StoreResponse](storageRef, Store(Instance.Record(appId = appId, config = config)))
              .map { res =>
                JsObject(
                  "id"      -> JsString(appId.value),
                  "result"  -> JsBoolean(res.ok)
                )
              }
              .andThen {
                case Success(x) =>
                  log.info("Successfully created app-instance {#{}}!", appId)
                  askApp[Instance.Config](appId, ReloadConfig())
              }
          )
        }} ~ die(`json body required`)
      } ~
      (path("delete") & `json/post`) {
        extract(ctx => ctx) { ctx => {
          die(`not implemented`)
        }}
      } ~
      (path("start") & `json/post`) {
        extract(ctx => ctx) { ctx => {
          import Instance.Commands.StartRequest

          // TODO(kudinkin): abstract
          complete(
            askApp[Instance.Status](appId, StartRequest())
              .map { v =>
                JsObject(
                  "id"      -> JsString(appId.value),
                  "result"  -> JsBoolean(true),
                  "status"  -> v.toJson.asJsObject)
              }
          )
        }}
      } ~
      (path("stop") & `json/post`) {
        extract(ctx => ctx) { ctx => {
          import Instance.Commands.StopRequest

          // TODO(kudinkin): abstract
          complete(
            askApp[Instance.Status](appId, StopRequest())
              .map { v =>
                JsObject(
                  "id"      -> JsString(appId.value),
                  "result"  -> JsBoolean(true),
                  "status"  -> v.toJson.asJsObject
                )
              }
          )
        }}
      }
  }

  override private[endpoints] def appRoute(appId: Instance.Id): Route = {
//    super.appRoute(appId) ~
      //authenticate(authenticator) {
      //  principal: Principal => {
      //    control(appId, principal)
      //  }
      //} ~
      control(appId, null)
  }

}


trait PublicEndpoint extends AppEndpoint {

  /**
    * Greps additional info about environment
    */
  private def mapAdditionalInfo(ip: RemoteAddress, userTs: Long): Map[String, String] = {
    Map(
      "ip"      -> ip.toOption.map { case ip => ip.getHostAddress }.getOrElse("0.0.0.0"),
      "userTs"  -> userTs.toString
    )
  }

  import Storage.Persisters._

  @inline
  private[endpoints] def predict(appId: Instance.Id, identity: UserIdentity): Future[(Predictor.Outcome, Instance.State)] = {
    import Instance.Commands.{PredictRequest, PredictResponse, predictTimeout}

    askApp[PredictResponse](appId, PredictRequest(identity))(timeout = predictTimeout)
      .map { r => (r.o, r.state) }
  }

  @inline
  private[endpoints] def info(appId: Instance.Id): Route = (path("info") & get) {
    respondWithMediaType(`application/json`) {
      complete("")
    }
  }

  @inline
  private[endpoints] def event(appId: Instance.Id): Route = pathPrefix("event") {
    import io.landy.app.model._
    `options/origin` ~
      (path("start") & `json/post`) {
        entity(as[JsObject]) { json => clientIP { ip => {
          val ev = json.convertTo[StartEvent]

          storeFor(appId, ev.copy(  appId     = appId,
                                    timestamp = System.currentTimeMillis(),
                                    identity  = ev.identity ++ mapAdditionalInfo(ip, ev.timestamp)))

          complete(
            Future {
              JsObject(
                "event" -> ev.toJson
              )
            }
          )
        }}} ~ die(`json body required`)
      } ~
      (path("predict") & `json/post`) {
        entity(as[JsObject]) { json => clientIP { ip => {
          val ev = json.convertTo[PredictEvent].copy(appId = appId)

          val identity = ev.identity ++ mapAdditionalInfo(ip, ev.timestamp)

          val prediction = predict(appId = ev.appId, identity = identity)

          complete(
            prediction
              .andThen {
                case Success((o, s)) => storeFor(appId, StartEvent( appId     = appId,
                                                                    session   = ev.session,
                                                                    timestamp = System.currentTimeMillis(),
                                                                    identity  = identity,
                                                                    variation = o.variation.id,
                                                                    kind      = StartEvent.deduceKindBy(o)))
              }
              .map { case (o, s) =>
                JsObject(
                  "variation" -> o.variation.toJson,
                  "predicted" -> o.isInstanceOf[Predictor.Outcome.Predicted].toJson
                )
              }
          )
        }}} ~ die(`json body required`)
      } ~
      (path("finish") & `json/post`) {
        entity(as[JsObject]) { json => {
          val ev: FinishEvent = json.convertTo[FinishEvent]

          storeFor(appId, ev.copy(appId     = appId,
                                  timestamp = System.currentTimeMillis()))

          complete("")
        }} ~ die(`json body required`)
      }
  }

  override private[endpoints] def appRoute(appId: Instance.Id): Route =  {
      info(appId) ~
        event(appId)
  }

  override private[endpoints] def route: Route =
    super[AppEndpoint].route /* ~ super[AppsEndpoint].route */
}


/**
  * Abstract (micro-)service handling particular HTTP endpoint
  */
trait Endpoint extends Service {

  private val appsRef = App.actor(Mediator.actorName)

  @inline
  private[endpoints] def askApps[T](message: Any)(implicit timeout: Timeout): Future[T] =
    ask[T](appsRef, message)

  @inline
  private[endpoints] def askApp[T](appId: Instance.Id, message: Instance.Message[T])(implicit timeout: Timeout): Future[T] =
    askApps[T](Mediator.Commands.Forward(appId, message))

  private[endpoints] def route: Route
}


/**
  * `/app/\*` endpoints
  */
trait AppEndpoint extends Endpoint {

  private[endpoints] val storageRef = App.actor(Storage.actorName)

  import Storage.Commands.{StoreResponse, Store}

  @inline
  private[endpoints] def storeFor[E](appId: Instance.Id, element: E)(implicit persister: Storage.PersisterW[E], timeout: Timeout): Future[StoreResponse] = {
    askApp[StoreResponse](appId, Store(element)(persister = persister))
  }

  override private[endpoints] def route: Route = {
    import Storage.padId
    pathPrefix("app" / Segment) {
      segment: String => {
        appRoute(Instance.Id(padId(segment)))
      }
    }
  }

  private[endpoints] def appRoute(appId: Instance.Id): Route
}


trait Service extends HttpService with JsonSupport
                                  with Logging
                                  with AskSupport
                                  with DefaultTimeout {

  implicit val context: ActorContext
  implicit val executionContext: ExecutionContext = context.dispatcher

  protected def complete(result: Future[JsObject]): Route = onComplete[JsObject](result) {
    case Success(j) => complete(j)
    case Failure(t) => failWith(t)
  }

  protected def die(message: String = "No route for"): Route = extract(ctx => ctx) { ctx => {
    complete(
      StatusCodes.BadRequest,
      JsString(s"$message: ${ctx.request.method} ${ctx.request.uri}").toString()
    )
  }}

  protected val `not implemented` = "Not implemented yet"
  protected val `json body required` = "Request body should be a JSON"

  protected val `Access-Control-Allow-Origin: *` = RawHeader("Access-Control-Allow-Origin", "*")
  protected val `json/post` = post & respondWithMediaType(`application/json`) & respondWithHeader(`Access-Control-Allow-Origin: *`)
  protected val `json/get` = get & respondWithMediaType(`application/json`) & respondWithHeader(`Access-Control-Allow-Origin: *`)

  protected val placeholder = reject // noop

  protected val `options/origin`: Route = options {
    headerValueByName("Origin") { origin: String => {
      respondWithHeaders(
        RawHeader("Access-Control-Allow-Origin", origin),
        RawHeader("Access-Control-Allow-Methods", "GET,POST"),
        RawHeader("Access-Control-Allow-Headers", "accept, content-type"),
        RawHeader("Access-Control-Max-Age", "1728000")
      ) {
        complete("")
      }
    }}
  }

  private def userAuth(in: Option[UserPass]): Future[Option[Principal]] = Future {
    in.flatMap(u => Some(new Principal(id = "0", name = u.user, pass = u.pass)))
  }

  protected val authenticator: HttpAuthenticator[Principal] = BasicAuth(
    authenticator = CachedUserPassAuthenticator(userAuth),
    realm = ""
  )

  implicit val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: spray.json.DeserializationException => respondWithHeader(`Access-Control-Allow-Origin: *`) {
      ctx => ctx.complete(StatusCodes.BadRequest, e.toString)
    }
    case e: Exception => respondWithHeader(`Access-Control-Allow-Origin: *`) {
      ctx => ctx.complete(StatusCodes.InternalServerError, e.toString)
    }
  }

}