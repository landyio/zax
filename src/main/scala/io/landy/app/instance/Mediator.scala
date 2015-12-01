package io.landy.app.instance

import akka.actor.{Props, ActorRef}
import io.landy.app.actors.ExecutingActor

/**
  * This actor is a 'mediator' of the whole apps orchestra
  */
class MediatorActor extends ExecutingActor {
  import Mediator._

  @inline
  private def appRef(appId: String): Option[ActorRef] = context.child(name = Instance.actorName(appId))

  @inline
  private def appRef(appId: String, forceStart: Boolean): Option[ActorRef] =
    appRef(appId).orElse(
      if (forceStart)
        Some(forceStartInstance(appId))
      else
        None
    )

  @inline
  private def startInstance(appId: String): Boolean = {
    appRef(appId, forceStart = true) match {
      case Some(_) => true
      case None => false
    }
  }

  @inline
  private def stopInstance(appId: String): Boolean = {
    appRef(appId) collect {
      case ref => context.stop(ref)
    }
    true
  }

  @inline
  private def forceStartInstance(appId: String): ActorRef = context.actorOf(
    props = Props(classOf[InstanceActor], appId),
    name = Instance.actorName(appId)
  )

  override def preStart(): Unit = {
    // TODO(kudinkin): pre-start instances?
  }

  def receive: Receive = trace {
      case Commands.Forward(appId, message) =>
        appRef(appId, forceStart = message.isInstanceOf[Instance.AutoStartMessage[_]]) match {
          case Some(appRef) => appRef.forward(message)
          case None         => sender ! akka.actor.Status.Failure(new NoSuchElementException(appId))
        }
  }

}

object Mediator {

  val actorName: String = "mediator"

  object Commands {
    case class Forward[R](appId: Instance.Id, message: Instance.Message[R])
  }

}
