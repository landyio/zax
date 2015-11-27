package io.landy.app.endpoints.serialization

import io.landy.app.instance.Instance
import io.landy.app.model._
import spray.httpx.SprayJsonSupport
import spray.json._

trait JsonSupport extends DefaultJsonProtocol with SprayJsonSupport {

  private def field[T](value: JsValue, fieldName: String, default: => T)(implicit reader: JsonReader[Option[T]]): T = {
    fromField[Option[T]](value, fieldName)(reader = reader).getOrElse(default)
  }

  private def field[T](value: JsValue, fieldName: String)(implicit reader: JsonReader[Option[T]]): Option[T] = {
    fromField[Option[T]](value, fieldName)(reader = reader)
  }

  /**
    * `UserIdentity`
    */
  private[endpoints] implicit object UserIdentityJsonFormat extends RootJsonFormat[UserIdentity] {

    override def write(o: UserIdentity): JsValue = o.params.toJson

    override def read(value: JsValue): UserIdentity = UserIdentity(value.convertTo[UserIdentity.Params])
  }

  /**
    * `Variation`
    */
  private[endpoints] implicit object VariationJsonFormat extends RootJsonFormat[Variation] {

    override def write(o: Variation): JsValue = o.id.toJson

    override def read(value: JsValue): Variation = Variation(value.convertTo[Variation.Id])
  }

  /**
    * `UserDataDescriptor`
    */
  private[endpoints] implicit object UserDataDescriptorJsonFormat extends RootJsonFormat[UserDataDescriptor] {

    import UserDataDescriptor._

    override def write(d: UserDataDescriptor): JsValue =
      JsObject(
        `name`        -> d.name.toJson,
        `categorical` -> d.categorical.toJson
      )

    override def read(value: JsValue): UserDataDescriptor = {
      { for (
          name  <- field[String]  (value, `name`);
          cat   <- field[Boolean] (value, `categorical`)
        ) yield UserDataDescriptor(name, cat)
      } get
    }
  }

  /**
    * `Instance.Config`
    */
  private[endpoints] implicit object AppInstanceConfigJsonFormat extends RootJsonFormat[Instance.Config] {

    def write(config: Instance.Config): JsValue = {
      import Instance.Config._

      JsObject(
        `variations`  -> config.variations.toJson,
        `descriptors` -> config.userDataDescriptors.toJson
      )
    }

    def read(value: JsValue): Instance.Config = {
      import Instance.Config._

      { for (
          vs <- field[Seq[Variation]]           (value, `variations`);
          ds <- field[Seq[UserDataDescriptor]]  (value, `descriptors`)
        ) yield Instance.Config(variations = vs, userDataDescriptors = ds, model = None)
      } get
    }
  }

  /**
   * `Instance.Status`
   */
  private[endpoints] implicit object AppInstanceStatusJsonFormat extends RootJsonFormat[Instance.Status] {
    def write(status: Instance.Status): JsObject =
      JsObject(
        "runState" -> JsString(status.runState.toString),
        "events" -> JsObject(
          "all" -> JsObject(
            "start" -> JsNumber(status.eventsAllStart),
            "finish" -> JsNumber(status.eventsAllFinish)
          )
        )
      )

    def read(value: JsValue): Instance.Status =
      Instance.Status.empty
  }

  /**
    * `StartEvent`
    */
  private[endpoints] implicit object StartEventJsonFormat extends RootJsonFormat[StartEvent] {
    import Event._

    def write(event: StartEvent): JsObject =
      JsObject(
        `type`      -> JsString("start"),
        `session`   -> JsString(event.session),
        `timestamp` -> JsNumber(event.timestamp),
        `identity`  -> event.identity.toJson,
        `variation` -> event.variation.toJson
      )

    def read(value: JsValue): StartEvent =
      StartEvent(
        session   = field[String]       (value, `session`,    ""),
        timestamp = field[Long]         (value, `timestamp`,  0l),
        identity  = field[UserIdentity] (value, `identity`,   UserIdentity.empty),
        variation = field[Variation]    (value, `variation`,  Variation.sentinel)
      )
  }

  /**
    * `PredictEvent`
    */
  private[endpoints] implicit object PredictEventJsonFormat extends RootJsonFormat[PredictEvent] {
    import Event._

    def write(event: PredictEvent): JsObject =
      JsObject(
        `type`      -> JsString("start"),
        `session`   -> JsString(event.session),
        `timestamp` -> JsNumber(event.timestamp),
        `identity`  -> event.identity.toJson
      )

    def read(value: JsValue): PredictEvent =
      PredictEvent(
        session   = field[String]       (value, `session`,    ""),
        timestamp = field[Long]         (value, `timestamp`,  0l),
        identity  = field[UserIdentity] (value, `identity`,   UserIdentity.empty )
      )
  }

  /**
    * `FinishEvent`
    */
  private[endpoints] implicit object FinishEventJsonFormat extends RootJsonFormat[FinishEvent] {
    import Event._

    def write(event: FinishEvent): JsObject = JsObject(
      `type` -> JsString("finish"),
      `session` -> JsString(event.session),
      `timestamp` -> JsNumber(event.timestamp)
    )

    def read(value: JsValue): FinishEvent = FinishEvent(
      session   = field[String](value, `session`, ""),
      timestamp = field[Long](value, `timestamp`, 0l)
    )
  }
}
