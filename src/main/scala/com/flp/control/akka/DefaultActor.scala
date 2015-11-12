package com.flp.control.akka

import akka.actor.Actor
import akka.util.Timeout

import scala.concurrent.ExecutionContext

trait DefaultTimeout {
  import scala.concurrent.duration._

  implicit val timeout: Timeout = 2.seconds
}

trait DefaultActor extends Actor with ActorTracing with DefaultTimeout {

  implicit val executionContext: ExecutionContext = context.dispatcher

}
