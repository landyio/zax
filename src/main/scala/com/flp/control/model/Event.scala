package com.flp.control.model

sealed trait Event {
  val appId: String
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
  override val appId: String = null,
  override val session: String,
  override val timestamp: Long,
  override val identity: UserIdentity
) extends PredictEventI



trait StartEventI extends Event {
  val identity: UserIdentity
  val variation: Variation
}

case class StartEvent(
  override val appId: String = null,
  override val session: String,
  override val timestamp: Long,
  override val identity: UserIdentity,
  override val variation: Variation
) extends StartEventI


trait FinishEventI extends Event

case class FinishEvent(
  override val appId: String = null,
  override val session: String,
  override val timestamp: Long
) extends FinishEventI
