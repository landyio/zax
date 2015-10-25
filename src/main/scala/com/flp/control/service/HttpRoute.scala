package com.flp.control.service

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.flp.control.akka.DefaultTimeout
import com.flp.control.boot.Boot
import com.flp.control.instance._
import com.flp.control.model._
import com.flp.control.params.ServerParams
import com.flp.control.storage.Storage
import spray.http.HttpHeaders.RawHeader
import spray.http.MediaTypes._
import spray.http._
import spray.httpx.SprayJsonSupport
import spray.json._
import spray.routing._
import spray.routing.authentication._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class PrivateHttpRouteActor extends HttpServiceActor with PrivateAppRoute {

  def receive = runRoute(route)
  val route: Route = appRoute

}

class PublicHttpRouteActor extends HttpServiceActor with PublicAppRoute {

  def receive = runRoute(route)
  val route: Route = appRoute

}


trait PrivateAppRoute extends PublicAppRoute {

  private implicit object AppInstanceConfigJsonFormat extends RootJsonFormat[AppInstanceConfig] {
    def write(config: AppInstanceConfig): JsValue = config.variants.toJson
    def read(value: JsValue): AppInstanceConfig = AppInstanceConfig(value.convertTo[Map[String, Seq[String]]])
  }

  private implicit object AppInstanceStatusJsonFormat extends RootJsonFormat[AppInstanceStatus] {
    def write(status: AppInstanceStatus): JsObject = JsObject(
      "runState" -> JsString(status.runState.toString),
      "events" -> JsObject(
        "all" -> JsObject(
          "start" -> JsNumber(status.eventsAllStart),
          "finish" -> JsNumber(status.eventsAllFinish)
        ),
        "learn" -> JsObject(
          "start" -> JsNumber(status.eventsLearnStart),
          "finish" -> JsNumber(status.eventsLearnFinish)
        )

      )
    )
    def read(value: JsValue): AppInstanceStatus = AppInstanceStatus.empty
  }

  @inline
  private[service] def control(appId: String, principal: Principal): Route = pathPrefix("control") {
    placeholder ~
      (path("config") & `json/get`) {
        extract(ctx => ctx) { ctx => {
          import AppInstance.Commands.{GetConfigRequest, GetConfigResponse}
          val result: Future[JsObject] = askApp[GetConfigResponse](appId, GetConfigRequest())
            .map { res => res.config }
            .map { v => v.toJson.asJsObject }
          complete(result)
        }}
      } ~
      (path("status") & `json/get`) {
        extract(ctx => ctx) { ctx => {
          import AppInstance.Commands.{GetStatusRequest, GetStatusResponse}
          val result: Future[JsObject] = askApp[GetStatusResponse](appId, GetStatusRequest())
            .map { res => res.status }
            .map { v => v.toJson.asJsObject }
          complete(result)
        }}
      } ~
      (path("create") & `json/post`) {
        entity(as[JsObject]) { json => {
          val config: AppInstanceConfig = json.convertTo[AppInstanceConfig]
          val result: Future[JsObject] = store(AppInstanceConfigRecord(appId = appId, config = config), asking = true)
            .map { res => JsObject("id" -> JsString(appId), "result" -> JsBoolean(res.ok)) }
          complete(result)
        }} ~ die(`json body required`)
      } ~
      (path("delete") & `json/post`) {
        extract(ctx => ctx) { ctx => {
          die(`not implemented`)
        }}
      } ~
      (path("start") & `json/post`) {
        extract(ctx => ctx) { ctx => {
          // TODO: XXX:
          import AppInstance.Commands.{GetStatusResponse, StartRequest}
          val result: Future[JsObject] = askApp[GetStatusResponse](appId, StartRequest())
            .map { res => res.status }
            .map { v => JsObject("id" -> JsString(appId), "result" -> JsBoolean(true), "status" -> v.toJson.asJsObject) }
          complete(result)
        }}
      } ~
      (path("stop") & `json/post`) {
        extract(ctx => ctx) { ctx => {
          // TODO: XXX:
          import AppInstance.Commands.{GetStatusResponse, StopRequest}
          val result: Future[JsObject] = askApp[GetStatusResponse](appId, StopRequest())
            .map { res => res.status }
            .map { v => JsObject("id" -> JsString(appId), "result" -> JsBoolean(true), "status" -> v.toJson.asJsObject) }
          complete(result)
        }}
      }
  }

  override private[service] def appRoute(appId: String): Route =  {
    super.appRoute(appId) ~
      //authenticate(authenticator) {
      //  principal: Principal => {
      //    control(appId, principal)
      //  }
      //} ~
      control(appId, null)
  }

}


trait PublicAppRoute extends AppRoute {

  private[service] implicit object StartEventJsonFormat extends RootJsonFormat[StartEvent] {
    import Event._
    def write(event: StartEvent): JsObject = JsObject(
      `type` -> JsString("start"),
      `session` -> JsString(event.session),
      `timestamp` -> JsNumber(event.timestamp),
      `identity` -> event.identity.toJson,
      `variation` -> event.variation.toJson
    )
    def read(value: JsValue): StartEvent = StartEvent(
      session = field[String](value, `session`, ""),
      timestamp = field[Long](value, `timestamp`, 0l),
      identity = field[IdentityData.Type](value, `identity`, IdentityData.empty ),
      variation = field[Variation.Type](value, `variation`, Variation.empty )
    )
  }

  private[service] implicit object PredictEventJsonFormat extends RootJsonFormat[PredictEvent] {
    import Event._
    def write(event: PredictEvent): JsObject = JsObject(
      `type` -> JsString("start"),
      `session` -> JsString(event.session),
      `timestamp` -> JsNumber(event.timestamp),
      `identity` -> event.identity.toJson
    )
    def read(value: JsValue): PredictEvent = PredictEvent(
      session = field[String](value, `session`, ""),
      timestamp = field[Long](value, `timestamp`, 0l),
      identity = field[IdentityData.Type](value, `identity`, IdentityData.empty )
    )
  }

  private[service] implicit object FinishEventJsonFormat extends RootJsonFormat[FinishEvent] {
    import Event._
    def write(event: FinishEvent): JsObject = JsObject(
      `type` -> JsString("finish"),
      `session` -> JsString(event.session),
      `timestamp` -> JsNumber(event.timestamp)
    )
    def read(value: JsValue): FinishEvent = FinishEvent(
      session = field[String](value, `session`, ""),
      timestamp = field[Long](value, `timestamp`, 0l)
    )
  }

  @inline
  private[service] def predict(appId: String, identity: IdentityData.Type): Future[Variation.Type] = {
    import AppInstance.Commands.{PredictRequest, PredictResponse, predictTimeout}
    askApp[PredictResponse](appId, PredictRequest(identity))(timeout = predictTimeout)
      .map { res => res.variation }
  }

  @inline
  private[service] def info(appId: String): Route = (path("info") & get) {
    respondWithMediaType(`application/json`) {
      complete("")
    }
  }

  @inline
  private[service] def event(appId: String): Route = pathPrefix("event") {
    import Storage._
    import com.flp.control.model._
    `options/origin` ~
      (path("start") & `json/post`) {
        entity(as[JsObject]) { json => clientIP { ip => {
          var ev: StartEvent = json.convertTo[StartEvent].copy(appId = appId)
          ev = ev.copy(identity = ServerParams.get(ev.identity, ip.toOption))
          store(ev)
          complete("")
        }}} ~ die(`json body required`)
      } ~
      (path("predict") & `json/post`) {
        entity(as[JsObject]) { json => clientIP { ip => {
          var ev: PredictEvent = json.convertTo[PredictEvent].copy(appId = appId)
          ev = ev.copy(identity = ServerParams.get(ev.identity, ip.toOption))
          val result: Future[Variation.Type] = predict(appId = ev.appId, identity = ev.identity)
          result.onSuccess { case v => store( StartEvent(appId = appId, session = ev.session, timestamp = ev.timestamp, identity = ev.identity, variation = v) ) }
          complete( result.map { v => v.toJson.asJsObject } )
        }}} ~ die(`json body required`)
      } ~
      (path("finish") & `json/post`) {
        entity(as[JsObject]) { json => {
          val ev: FinishEvent = json.convertTo[FinishEvent].copy(appId = appId)
          store(ev)
          complete("")
        }} ~ die(`json body required`)
      }
  }

  override private[service] def appRoute(appId: String): Route =  {
      info(appId) ~
        event(appId)
  }

}

trait AppRoute extends Service {

  private[service] val appsRef = Boot.actor(AppInstances.actorName)
  private[service] val storageRef = Boot.actor(Storage.actorName)

  @inline
  private[service] def store[E](element: E, asking: Boolean = false)(implicit persister: Storage.PersisterW[E], timeout: Timeout): Future[Storage.Commands.StoreResponse] = {
    val message = Storage.Commands.Store(element, asking)( persister = persister )
    if (asking) {
      storageRef.ask(message).map { res => res.asInstanceOf[ Storage.Commands.StoreResponse ] }
    } else {
      storageRef ! message
      null  // there is no future here :)
            // OLD: return Promise.failed(new UnsupportedOperationException()).future
    }
  }

  @inline
  private[service] def store[E](element: E)(implicit persister: Storage.PersisterW[E]): Unit = {
    storageRef ! Storage.Commands.Store(element)( persister = persister )
  }

  @inline
  private[service] def askApps[T](message: Any)(implicit timeout: Timeout): Future[T] =
    appsRef.ask(message).map { res => res.asInstanceOf[T] }

  @inline
  private[service] def askApp[T](appId: String, message: AppInstanceMessage[T])(implicit timeout: Timeout): Future[T] =
    askApps[T]( AppInstances.Commands.Forward(appId, message) )

  private[service] val appRoute: Route = pathPrefix("app" / Segment) {
    segment: String => {
      val appId = AppInstance.fixId(segment)
      appRoute(appId)
    }
  }

  private[service] def appRoute(appId: String): Route
}


trait Service extends HttpService with SprayJsonSupport with DefaultJsonProtocol with DefaultTimeout {

  implicit val context: ActorContext
  implicit val executionContext: ExecutionContext = context.dispatcher

  protected def field[T](value: JsValue, fieldName: String, default: => T)(implicit reader: JsonReader[Option[T]]): T = {
    fromField[Option[T]](value, fieldName) (reader = reader) .getOrElse(default)
  }

  protected def field[T](value: JsValue, fieldName: String)(implicit reader: JsonReader[Option[T]]): T = {
    fromField[Option[T]](value, fieldName) (reader = reader) .get
  }

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

object HttpRoute {
  val privActorName: String = "http-private"
  val publActorName: String = "http-public"
}