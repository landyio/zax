package com.flp.control.instance

import akka.actor.{Props, ActorRef}
import com.flp.control.akka.DefaultActor

/**
  * This actor is a 'mediator' of the whole apps orchestra
  */
class AppInstancesActor extends DefaultActor {
  import AppInstances._

  @inline
  private def appRef(appId: String): Option[ActorRef] = context.child(name = AppInstance.actorName(appId))

  @inline
  private def appRef(appId: String, forceStart: Boolean): Option[ActorRef] =
    appRef(appId).orElse(
      if (forceStart)
        Some(forceStartAppInstance(appId))
      else
        None
    )

  @inline
  private def startAppInstance(appId: String): Boolean = {
    appRef(appId, forceStart = true) match {
      case Some(_) => true
      case None => false
    }
  }

  @inline
  private def stopAppInstance(appId: String): Boolean = {
    appRef(appId) match {
      case Some(ref) => context.stop(ref)
    }
    true
  }

  @inline
  private def forceStartAppInstance(appId: String): ActorRef = context.actorOf(
    props = Props(classOf[AppInstanceActor], appId),
    name = AppInstance.actorName(appId)
  )

  override def preStart(): Unit = {
    // TODO: read AppInstances, forEach(appId => self ? Control.Start.Request(appId))
  }

  def receive: Receive = trace {
      case Commands.Forward(appId, message) =>
        appRef(appId, forceStart = message.isInstanceOf[AppInstanceAutoStartMessage[_]]) match {
          case Some(appRef) => appRef.forward(message)
          case None         => sender ! akka.actor.Status.Failure(new NoSuchElementException(appId))
        }
  }

}

object AppInstances {

  val actorName: String = "app-instances"

  object Commands {
    case class Forward[R](appId: String, message: AppInstanceMessage[R])
  }

}
