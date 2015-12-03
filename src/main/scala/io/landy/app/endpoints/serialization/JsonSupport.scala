package io.landy.app.endpoints.serialization

import io.landy.app.instance.Instance
import io.landy.app.model._
import io.landy.app.storage.Storage
import spray.httpx.SprayJsonSupport
import spray.json._

import scala.language.postfixOps

trait JsonSupport extends DefaultJsonProtocol with SprayJsonSupport {

  private def field[T](value: JsValue, fieldName: String, default: => T)(implicit reader: JsonReader[Option[T]]): T =
    fromField[Option[T]](value, fieldName)(reader = reader).getOrElse(default)

  private def field[T](value: JsValue, fieldName: String)(implicit reader: JsonReader[Option[T]]): Option[T] =
    fromField[Option[T]](value, fieldName)(reader = reader)

  /**
    * `UserIdentity`
    */
  private[endpoints] implicit object UserIdentityJsonFormat extends RootJsonFormat[UserIdentity] {

    override def write(o: UserIdentity): JsValue = o.params.toJson

    override def read(value: JsValue): UserIdentity = UserIdentity(value.convertTo[UserIdentity.Params])
  }

  /**
    * `Variation.Id`
    */
  private implicit object VariationIdJsonFormat extends RootJsonFormat[Variation.Id] {
    import Storage.padId

    override def read(json: JsValue): Variation.Id =
      Variation.Id(
        padId(json.asInstanceOf[JsString].value)
      )

    override def write(obj: Variation.Id): JsValue =
      JsString(obj.value)
  }

  /**
    * `Variation`
    */
  private[endpoints] implicit object VariationJsonFormat extends RootJsonFormat[Variation] {

    import Variation._

    override def write(o: Variation): JsValue =
      JsObject(
        `value` -> o.value.toJson,
        `id`    -> o.id.toJson
      )

    override def read(value: JsValue): Variation = {
      { for (
          id    <- field[Variation.Id]   (value, Variation.`id`);
          value <- field[Variation.Type] (value, Variation.`value`)
        ) yield Variation(id, value)
      } get
    }
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
  private[endpoints] implicit object InstanceConfigJsonFormat extends RootJsonFormat[Instance.Config] {

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
  private[endpoints] implicit object InstanceStatusJsonFormat extends RootJsonFormat[Instance.Status] {
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
        `type`      -> JsString(`type:Start`),
        `session`   -> JsString(event.session),
        `timestamp` -> JsNumber(event.timestamp),
        `identity`  -> event.identity.toJson,
        `variation` -> event.variation.toJson
      )

    def read(value: JsValue): StartEvent =
      StartEvent(
        session   = field[String]       (value, `session`)    .get, /* mandatory */
        variation = field[Variation.Id] (value, `variation`)  .get, /* mandatory */
        identity  = field[UserIdentity] (value, `identity`,   UserIdentity.empty),
        timestamp = field[Long]         (value, `timestamp`,  0l),

        // Events are sampled by default
        kind = StartEvent.Kind.Random
      )
  }

  /**
    * `PredictEvent`
    */
  private[endpoints] implicit object PredictEventJsonFormat extends RootJsonFormat[PredictEvent] {
    import Event._

    def write(event: PredictEvent): JsObject =
      JsObject(
        `type`      -> JsString(`type:Predict`),
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
      `type`      -> JsString(`type:Finish`),
      `session`   -> JsString(event.session),
      `timestamp` -> JsNumber(event.timestamp)
    )

    def read(value: JsValue): FinishEvent = FinishEvent(
      session   = field[String](value, `session`, ""),
      timestamp = field[Long](value, `timestamp`, 0l)
    )
  }
}
