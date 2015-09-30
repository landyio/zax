package com.flp.control.akka

import akka.actor.Actor

import scala.concurrent.ExecutionContext

trait DefaultActor extends Actor with ActorTracing with DefaultTimeout {
  implicit val executionContext: ExecutionContext = context.dispatcher

}
