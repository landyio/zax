package com.flp.control.akka

import akka.actor.{Actor, ActorLogging}
import com.typesafe.config.{Config, ConfigFactory}

trait ActorTracing extends ActorLogging {
  this: Actor =>

  private val enabled: Boolean = log.isInfoEnabled && ActorTracing.enabled

  protected def trace(receive: Receive): Receive = {
    return trace("")(receive)
  }

  protected def trace(prefix: String)(receive: Receive): Receive = {
    if (!enabled) { return receive }

    return new Receive {

      override def isDefinedAt(o: Any): Boolean = {
        val definedAt: Boolean = receive.isDefinedAt(o)
        if (!definedAt) {
          log.info(s"${prefix}${String.valueOf(o)}::undefined (${o.getClass().getName()}})")
        }
        return definedAt
      }

      override def apply(o: Any): Unit = {
        log.info(s"${prefix}${String.valueOf(o)}")
        receive.apply(o)
      }

    }
  }

}

private object ActorTracing {

  private val param: String = "flp.akka.trace"
  private val config: Config = ConfigFactory.load()
  private val enabled: Boolean = config.hasPath(param) && config.getBoolean(param)

}