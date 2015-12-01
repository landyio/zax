package io.landy.app.model

import io.landy.app.instance.Instance

sealed trait Event {
  val appId: Instance.Id
  val session: String
  val timestamp: Long
}

object Event {
  val `appId` = "appId"
  val `type` = "type"
  val `type:Start` = "start"
  val `type:Finish` = "finish"
  val `session` = "session"
  val `timestamp` = "timestamp"
  val `identity` = "identity"
  val `variation` = "variation"
}


trait PredictEventI extends Event {
  val identity: UserIdentity
}

case class PredictEvent(
  override val appId: Instance.Id = null,
  override val session: String,
  override val timestamp: Long,
  override val identity: UserIdentity
) extends PredictEventI



trait StartEventI extends Event {
  val identity: UserIdentity
  val variation: Variation.Id
}

case class StartEvent(
  override val appId: Instance.Id = null,
  override val session: String,
  override val timestamp: Long,
  override val identity: UserIdentity,
  override val variation: Variation.Id
) extends StartEventI


trait FinishEventI extends Event

case class FinishEvent(
  override val appId: Instance.Id = null,
  override val session: String,
  override val timestamp: Long
) extends FinishEventI
