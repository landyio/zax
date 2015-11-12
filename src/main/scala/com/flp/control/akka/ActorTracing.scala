package com.flp.control.akka

import akka.actor.{Actor, ActorLogging}
import com.typesafe.config.ConfigFactory

trait ActorTracing extends ActorLogging { this: Actor =>

  import ActorTracing.TRACE_ASCRIPTION

  private val enabled: Boolean = log.isInfoEnabled && ActorTracing.enabled

  protected def trace(receive: Receive): Receive = trace(TRACE_ASCRIPTION)(receive)

  protected def trace(prefix: String)(receive: Receive): Receive =
    if (!enabled)
      receive
    else
      new Receive {

        override def isDefinedAt(o: Any): Boolean = {
          val definedAt: Boolean = receive.isDefinedAt(o)
          if (!definedAt) {
            log.info(s"[$prefix] '${self.path}' is undefined at: ${String.valueOf(o)}::undefined (${o.getClass.getName}})")
          }
          definedAt
        }

        override def apply(o: Any): Unit = {
          log.info(s"[$prefix] '${self.path}' received: ${String.valueOf(o)}")
          receive.apply(o)
        }

      }

}

private object ActorTracing {

  private val TRACE_ASCRIPTION: String = "TRC"

  private val param   = "flp.akka.trace"
  private val config  = ConfigFactory.load()

  private val enabled = config.hasPath(param) && config.getBoolean(param)

}