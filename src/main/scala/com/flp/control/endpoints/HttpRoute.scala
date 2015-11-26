package com.flp.control.endpoints

import akka.actor._
import akka.util.Timeout
import com.flp.control.App
import com.flp.control.actors.{Logging, AskSupport, DefaultTimeout}
import com.flp.control.instance._
import com.flp.control.model._
import com.flp.control.endpoints.serialization.JsonSupport
import com.flp.control.storage.Storage
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
  val route: Route = appRoute

}

class PublicEndpointActor extends HttpServiceActor  with PublicEndpoint
                                                    with ActorLogging {

  def receive = runRoute(route)
  val route: Route = appRoute

}


trait PrivateEndpoint extends PublicEndpoint {

  import Storage.Persisters._

  @inline
  private[endpoints] def control(appId: String, principal: Principal): Route = pathPrefix("control") {
    placeholder ~
      (path("config") & `json/get`) {
        extract(ctx => ctx) { ctx => {
          import AppInstance.Commands.ConfigRequest

          complete(
            askApp[AppInstanceConfig](appId, ConfigRequest())
              .map { v => v.toJson.asJsObject }
          )
        }}
      } ~
      (path("status") & `json/get`) {
        extract(ctx => ctx) { ctx => {
          import AppInstance.Commands.StatusRequest

          complete(
            askApp[AppInstanceStatus](appId, StatusRequest())
              .map { v => v.toJson.asJsObject }
          )
        }}
      } ~
      (path("create") & `json/post`) {

        //
        // TODO(kudinkin): move to `/apps/create`
        //

        entity(as[JsObject]) { json => {
          val config = json.convertTo[AppInstanceConfig]

          import AppInstance.Commands.ReloadConfig

          complete(
            store(AppInstanceConfig.Record(appId = appId, config = config))
              .map { res =>
                JsObject(
                  "id"      -> JsString(appId),
                  "result"  -> JsBoolean(res.ok)
                )
              }
              .andThen {
                case Success(x) =>
                  log.info("Successfully created app-instance {#{}}!", appId)
                  askApp[AppInstanceConfig](appId, ReloadConfig())
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
          import AppInstance.Commands.StartRequest

          // TODO(kudinkin): abstract
          complete(
            askApp[AppInstanceStatus](appId, StartRequest())
              .map { v =>
                JsObject(
                  "id"      -> JsString(appId),
                  "result"  -> JsBoolean(true),
                  "status"  -> v.toJson.asJsObject)
              }
          )
        }}
      } ~
      (path("stop") & `json/post`) {
        extract(ctx => ctx) { ctx => {
          import AppInstance.Commands.StopRequest

          // TODO(kudinkin): abstract
          complete(
            askApp[AppInstanceStatus](appId, StopRequest())
              .map { v =>
                JsObject(
                  "id"      -> JsString(appId),
                  "result"  -> JsBoolean(true),
                  "status"  -> v.toJson.asJsObject
                )
              }
          )
        }}
      }
  }

  override private[endpoints] def appRoute(appId: String): Route = {
    super.appRoute(appId) ~
      //authenticate(authenticator) {
      //  principal: Principal => {
      //    control(appId, principal)
      //  }
      //} ~
      control(appId, null)
  }

}


trait PublicEndpoint extends Endpoint {

  import Storage.Persisters._

  @inline
  private[endpoints] def predict(appId: String, identity: UserIdentity): Future[Variation] = {
    import AppInstance.Commands.{PredictRequest, PredictResponse, predictTimeout}

    askApp[PredictResponse](appId, PredictRequest(identity))(timeout = predictTimeout)
      .map { r => r.variation }
  }

  @inline
  private[endpoints] def info(appId: String): Route = (path("info") & get) {
    respondWithMediaType(`application/json`) {
      complete("")
    }
  }

  @inline
  private[endpoints] def event(appId: String): Route = pathPrefix("event") {
    import com.flp.control.model._
    `options/origin` ~
      (path("start") & `json/post`) {
        entity(as[JsObject]) { json => clientIP { ip => {
          val ev = json.convertTo[StartEvent]

          val additional = Map {
            "ip"        -> ip.toOption.map { case ip => ip.getHostAddress }.getOrElse("0.0.0.0")
            "serverTs"  -> System.currentTimeMillis().toString
          }

          store(ev.copy(appId = appId, identity = ev.identity ++ additional))

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

          val additional = Map {
            "ip"        -> ip.toOption.map { case ip => ip.getHostAddress }.getOrElse("0.0.0.0")
            "serverTs"  -> System.currentTimeMillis().toString
          }

          val prediction = predict(appId = ev.appId, identity = ev.identity ++ additional)

          complete(
            prediction
              .andThen {
                case Success(v) => store(StartEvent(appId, ev.session, ev.timestamp, ev.identity, variation = v))
              }
              .map { v =>
                JsObject("variation" -> v.toJson)
              }
          )
        }}} ~ die(`json body required`)
      } ~
      (path("finish") & `json/post`) {
        entity(as[JsObject]) { json => {
          val ev: FinishEvent = json.convertTo[FinishEvent]

          store(ev.copy(appId = appId))

          complete("")
        }} ~ die(`json body required`)
      }
  }

  override private[endpoints] def appRoute(appId: String): Route =  {
      info(appId) ~
        event(appId)
  }

}

trait Endpoint extends Service {

  private val appsRef    = App.actor(AppInstances.actorName)
  private val storageRef = App.actor(Storage.actorName)

  import Storage.Commands.{StoreResponse, Store}

  @inline
  private[endpoints] def store[E](element: E)(implicit persister: Storage.PersisterW[E], timeout: Timeout): Future[StoreResponse] = {
    ask[StoreResponse](storageRef, Store(element)(persister = persister))
      .andThen {
        case Success(r) =>
          log.debug("Stored '{}' ({}) instance / {{}}", element.getClass.getName, element.hashCode(), element)
      }
  }

  @inline
  private[endpoints] def askApps[T](message: Any)(implicit timeout: Timeout): Future[T] =
    ask[T](appsRef, message)

  @inline
  private[endpoints] def askApp[T](appId: String, message: AppInstanceMessage[T])(implicit timeout: Timeout): Future[T] =
    askApps[T](AppInstances.Commands.Forward(appId, message))

  private[endpoints] val appRoute: Route = pathPrefix("app" / Segment) {
    segment: String => {
      val appId = AppInstance.fixId(segment)
      appRoute(appId)
    }
  }

  private[endpoints] def appRoute(appId: String): Route
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