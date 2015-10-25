package com.flp.control.model

import com.flp.control.instance._

object IdentityData {
  type Type = Map[String, String]
  def apply(params: Map[String, String]) = params
  val empty: Type = apply(Map())
}

object Variation {
  type Type = Map[String, String]
  def apply(params: Map[String, String]) = params
  val empty: Type = apply(Map())
}

case class AppInstanceConfigRecord(
  appId: String,
  runState: AppInstanceRunState.Value = AppInstanceRunState.Stopped,
  config: AppInstanceConfig = AppInstanceConfig.empty
)

object AppInstanceConfigRecord {
  val `config` = "config"
  val `runState` = "runState"
  def notFound(appId: String) = AppInstanceConfigRecord(
    appId = appId,
    runState = AppInstanceRunState.NoData,
    config = AppInstanceConfig.empty
  )
}