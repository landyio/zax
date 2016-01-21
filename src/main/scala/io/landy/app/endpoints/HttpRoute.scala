package io.landy.app.endpoints

import akka.actor._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.landy.app.actors.{Logging, AskSupport, DefaultTimeout}
import io.landy.app.instance.Instance.Id
import io.landy.app.instance._
import io.landy.app.model._
import io.landy.app.endpoints.serialization.JsonSupport
import io.landy.app.storage.Storage
import io.landy.app.App
import io.landy.app.util.geo.Resolver
import spray.http.HttpHeaders.{`WWW-Authenticate`, RawHeader}
import spray.http.MediaTypes._
import spray.http._
import spray.json._
import spray.routing._
import spray.routing.authentication._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class PrivateEndpointActor extends HttpServiceActor with PrivateEndpoint
                                                    with ActorLogging {

  val config = ConfigFactory.load()

  override def isAuthenticationEnabled: Boolean = config.getBoolean("spray.routing.private.authentication.enabled")

  def receive = runRoute(route)
}

class PublicEndpointActor extends HttpServiceActor  with PublicEndpoint
                                                    with ActorLogging {
  def receive = runRoute(route)
}


trait PrivateEndpoint extends AppEndpoint {

  import Storage.Persisters._

  def isAuthenticationEnabled: Boolean

  @inline
  private[endpoints] def control(appId: Instance.Id, p: Principal): Route =
    pathPrefix("control") {
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
          }} ~ die(`json required`)
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

  @inline
  private[endpoints] def samples(appId: Instance.Id, p: Principal): Route = {
    import Instance.Commands.{DumpSampleRequest, DumpSampleResponse}

    pathPrefix("samples") {
      placeholder ~ (path("dump") & `json/get`) {
        extract(identity) { ctx =>
          complete(
            askApp[DumpSampleResponse](appId, DumpSampleRequest())(30.seconds)
              .map { r =>
                JsObject(
                  "url" -> JsString(r.url)
                )
              }
          )
        }
      }
    }
  }

  override private[endpoints] def appRoute(appId: Instance.Id): Route = {
    if (isAuthenticationEnabled)
      authenticate(authenticator) { p =>
          forwardRoute(appId, p)
      }
    else
      forwardRoute(appId, null)
  }

  def forwardRoute(appId: Id, p: Principal): Route = {
    control(appId, p) ~
      samples(appId, p)
  }
}


trait PublicEndpoint extends AppEndpoint {

  /**
    * Greps additional info about environment
    */
  private def mapAdditionalInfo(addr: RemoteAddress, userTs: Long): Map[String, String] = {
    val oip = addr.toOption
    Map(
      Seq("userTs" -> userTs.toString) ++
      oip .map {
            case ip =>
              (Resolver(ip) match {
                case Resolver.City(name, country) => Seq("city" -> name, "country" -> country)
              }) ++ Seq("ip" -> ip.getHostAddress)
          }
          .getOrElse(
            Seq("ip" -> "0.0.0.0")
          ): _*
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
        }}} ~ die(`json required`)
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
        }}} ~ die(`json required`)
      } ~
      (path("finish") & `json/post`) {
        entity(as[JsObject]) { json => {
          val ev: FinishEvent = json.convertTo[FinishEvent]

          storeFor(appId, ev.copy(appId     = appId,
                                  timestamp = System.currentTimeMillis()))

          complete("")
        }} ~ die(`json required`)
      }
  }

  override private[endpoints] def appRoute(appId: Instance.Id): Route =
    event(appId)

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
      id => {
        appRoute(Instance.Id(padId(id)))
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

  protected val `not implemented` = "Not Implemented"
  protected val `json required`   = "Request body should be a valid JSON object"

  protected val `Access-Control-Allow-Origin: *` = RawHeader("Access-Control-Allow-Origin", "*")

  protected val `json/post` = post  & respondWithMediaType(`application/json`) & respondWithHeader(`Access-Control-Allow-Origin: *`)
  protected val `json/get`  = get   & respondWithMediaType(`application/json`) & respondWithHeader(`Access-Control-Allow-Origin: *`)

  protected val placeholder = reject

  protected val `options/origin`: Route = options {
    headerValueByName("Origin") {
      origin: String => {
        respondWithHeaders(
          RawHeader("Access-Control-Allow-Origin", origin),
          RawHeader("Access-Control-Allow-Methods", "GET,POST"),
          RawHeader("Access-Control-Allow-Headers", "accept, content-type"),
          RawHeader("Access-Control-Max-Age", "1728000")
        ) { complete("") }
      }
    }
  }

  private val auth = new {
    private val conf = ConfigFactory.load()

    val scheme  = conf.getString("spray.routing.private.authentication.scheme")
    val token   = conf.getString("spray.routing.private.authentication.token")
  }

  type TokenAuthenticator[T] = Option[(String, String)] => Future[Option[T]]

  case class BasicTokenAuthenticator(
    realm: String,
    tokenAuthenticator: TokenAuthenticator[Principal]
  )(implicit val executionContext: ExecutionContext) extends HttpAuthenticator[Principal] {

    override def authenticate(credentials: Option[HttpCredentials], ctx: RequestContext): Future[Option[Principal]] =
      tokenAuthenticator {
        credentials.flatMap {
          case GenericHttpCredentials(scheme, token, _) => Some((scheme, token))
          case _                                        => None
        }
      }

    override def getChallengeHeaders(httpRequest: HttpRequest): List[HttpHeader] =
      `WWW-Authenticate`(HttpChallenge("Token", realm = realm, params = Map.empty)) :: Nil
  }

  protected val authenticator =
    BasicTokenAuthenticator(
      realm = "",
      tokenAuthenticator =
        params =>
          Future {
            params.flatMap {
              case (scheme, token) =>
                if (auth.scheme.equalsIgnoreCase(scheme) && auth.token.equals(token)) Some(God)
                else                                                                  None
            }
          }
    )

  /**
    * Exception-handler for the endpoints
    */
  implicit val exceptionHandler: ExceptionHandler = ExceptionHandler {

    case e: spray.json.DeserializationException =>
      respondWithHeader(`Access-Control-Allow-Origin: *`) {
        _.complete(StatusCodes.BadRequest, e.toString)
      }

    case e: Exception =>
      respondWithHeader(`Access-Control-Allow-Origin: *`) {
        _.complete(StatusCodes.InternalServerError, e.toString)
      }
  }

}