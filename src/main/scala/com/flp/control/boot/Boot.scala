package com.flp.control.boot

import java.io.FileNotFoundException

import akka.actor._
import akka.io.IO
import akka.pattern.ask
import com.flp.control.akka.{ActorTracing, DefaultTimeout}
import com.typesafe.config.ConfigFactory
import spray.can.Http
import spray.can.server.ServerSettings

import scala.concurrent.duration._

class BootActor extends Actor with ActorTracing with DefaultTimeout {

  import Boot._

  @inline
  private def startStorage(): Unit = {
    import com.flp.control.storage.{Storage, StorageActor}
    val ref: ActorRef = context.actorOf(
      props = Props[StorageActor],
      name = Storage.actorName
    )
  }

  @inline
  private def startAppInstances(): Unit = {
    import com.flp.control.instance.{AppInstances, AppInstancesActor}
    val ref: ActorRef = context.actorOf(
      props = Props[AppInstancesActor],
      name = AppInstances.actorName
    )
  }

  object SSLEngine {

    import javax.net.ssl.SSLContext

    implicit lazy val sslContext: SSLContext = {
      import java.security.{KeyStore, SecureRandom}
      import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

      import scala.reflect.io.File

      val conf = ConfigFactory.load()

      val keyStoreResource: String  = conf.getString("ssl.certificate-file")
      val password: String          = conf.getString("ssl.certificate-password")

      val keyStore = KeyStore.getInstance("JKS")

      val ks = File(keyStoreResource)
      try {
        val s = ks.inputStream()
        try {
          keyStore.load(s, password.toCharArray)
        } finally {
          s.close()
        }
      } catch {
        case fnf: FileNotFoundException => log.error(fnf, "KeyStore not found!")
      }

      val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
      keyManagerFactory.init(keyStore, password.toCharArray)

      val trustManagerFactory = TrustManagerFactory.getInstance("SunX509")
      trustManagerFactory.init(keyStore)

      val context = SSLContext.getInstance("TLS")
      context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
      context
    }

    import spray.io.ServerSSLEngineProvider

    implicit lazy val engineProvider = ServerSSLEngineProvider {
      engine => {
        // engine.setEnabledProtocols(Array("SSLv3", "TLSv1"))
        engine
      }
    }
  }

  @inline
  private def startHttp(): Unit = {

    val conf = ConfigFactory.load()

    val privateHost: String = conf.getString("flp.server.private.host")
    val privatePort: Int = conf.getInt("flp.server.private.port")

    val publicHost: String = conf.getString("flp.server.public.host")
    val publicPort: Int = conf.getInt("flp.server.public.port")

    val privateHttps: Boolean = conf.getBoolean("flp.server.private.https")
    val publicHttps: Boolean = conf.getBoolean("flp.server.public.https")

    import com.flp.control.service._

    val privateRef: ActorRef = context.actorOf(
      props = Props[PrivateHttpRouteActor],
      name = HttpRoute.privActorName
    )

    val publicRef: ActorRef = context.actorOf(
      props = Props[PublicHttpRouteActor],
      name = HttpRoute.publActorName
    )

    implicit val system: ActorSystem = context.system

    import spray.can.server.ServerSettings
    val settingsHttp = ServerSettings(system).copy(
      remoteAddressHeader = true, // this is required for client-ip resolution
      sslEncryption = false // no-ssl
    )

    val settingsHttps = settingsHttp.copy(
      sslEncryption = true // have-ssl
    )

    import spray.can.Http

    //
    // Private
    //
    IO(Http) ? bind(privateHost, privatePort, privateRef, if (privateHttps) settingsHttps else settingsHttp, privateHttps)


    //
    // Public
    //
    IO(Http) ? bind(publicHost, publicPort, publicRef, if (publicHttps) settingsHttps else settingsHttp, publicHttps)
  }


  def bind(host: String, port: Int, listener: ActorRef, settings: ServerSettings, useHttps: Boolean) =
    useHttps match {
      case true =>
        import SSLEngine._
        Http.Bind(listener = listener, interface = host, port = port, settings = Some(settings))

      case false =>
        Http.Bind(listener = listener, interface = host, port = port, settings = Some(settings))
    }

  def receive: Receive = trace {
    case Commands.Startup() => {
      startStorage()
      startAppInstances()
      startHttp()
      sender ! true
    }
    case Commands.Shutdown() => {
      context.children.foreach(actor => context.stop(actor))
      sender ! true
    }
  }
}

object Boot extends DefaultTimeout {

  val bootActorName = "boot"
  def actor(path: ActorPath)(implicit context: ActorContext): ActorRef = context.actorFor(path)
  def actor(path: String)(implicit context: ActorContext): ActorRef = actor(context.system / bootActorName / path)

  object Commands {
    case class Startup()
    case class Shutdown()
  }

  implicit val system = ActorSystem("flp")

  sys.addShutdownHook { system.shutdown() }

  def main(args: Array[String]): Unit = {
    val bootRef: ActorRef = system.actorOf(Props[BootActor], name = bootActorName)
    system.registerOnTermination { bootRef ! Commands.Shutdown() }

    import scala.concurrent.Await
    Await.ready(bootRef ? Commands.Startup(), 60.seconds)

    system.log.info("Started")
  }

}

