package io.landy.app.instance

import akka.actor.{Props, ActorRef}
import io.landy.app.actors.ExecutingActor

/**
  * This actor is a 'mediator' of the whole apps orchestra
  */
class MediatorActor extends ExecutingActor {
  import Mediator._

  @inline
  private def appRef(appId: Instance.Id): Option[ActorRef] = context.child(name = Instance.actorName(appId))

  @inline
  private def appRef(appId: Instance.Id, forceStart: Boolean): Option[ActorRef] =
    appRef(appId).orElse(
      if (forceStart)
        Some(forceStartInstance(appId))
      else
        None
    )

  @inline
  private def startInstance(appId: Instance.Id): Boolean = {
    appRef(appId, forceStart = true) match {
      case Some(_) => true
      case None => false
    }
  }

  @inline
  private def stopInstance(appId: Instance.Id): Boolean = {
    appRef(appId) collect {
      case ref => context.stop(ref)
    }
    true
  }

  @inline
  private def forceStartInstance(appId: Instance.Id): ActorRef = context.actorOf(
    // Supplied `Instance.Config.empty` is a stub: actor will pre-load
    // own (proper) config from db upon start-up, or will respond
    // with failure taking poison-pill
    props = Props(classOf[InstanceActor], appId, Instance.Config.empty),
    name = Instance.actorName(appId)
  )

  override def preStart(): Unit = {
    // TODO(kudinkin): pre-start instances?
  }

  def receive: Receive = trace {
      case Commands.Forward(appId, message) =>
        appRef(appId, forceStart = message.isInstanceOf[Instance.AutoStartMessage[_]]) match {
          case Some(appRef) => appRef.forward(message)
          case None         => sender ! akka.actor.Status.Failure(new NoSuchElementException(appId.value))
        }
  }

}

object Mediator {

  val actorName: String = "mediator"

  object Commands {
    case class Forward[R](appId: Instance.Id, message: Instance.Message[R])
  }

}
