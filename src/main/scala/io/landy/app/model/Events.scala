package io.landy.app.model

import io.landy.app.instance.Instance

sealed trait Event {
  val appId:    Instance.Id
  val session:  String
  val timestamp: Long
}

object Event {
  val `appId` = "appId"

  val `type`          = "type"
  val `type:Start`    = "start"
  val `type:Finish`   = "finish"
  val `type:Predict`  = "predict"

  val `kind`            = "kind"
  val `kind:Predicted`  = "predicted"
  val `kind:Random`     = "random"

  val `timestamp` = "timestamp"

  val `session`   = "session"
  val `identity`  = "identity"
  val `variation` = "variation"
}


case class PredictEvent(
  override val  appId:     Instance.Id = null,
  override val  session:   String,
  override val  timestamp: Long,
                identity:  UserIdentity
) extends Event


case class StartEvent(
  override val  appId:      Instance.Id = null,
  override val  session:    String,
  override val  timestamp:  Long,
                identity:   UserIdentity,
                variation:  Variation.Id,
                kind:       StartEvent.Kind.Type

) extends Event

object StartEvent {

  /**
    * Designates whether particular event was sampled one
    * or predicted one
    */
  object Kind extends Enumeration {
    type Type = Value
    val Predicted, Random = Value
  }
}


case class FinishEvent(
  override val appId:     Instance.Id = null,
  override val session:   String,
  override val timestamp: Long
) extends Event
