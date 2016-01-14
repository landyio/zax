package io.landy.app

import java.io.FileNotFoundException

import akka.actor._
import akka.io.IO
import akka.util.Timeout
import io.landy.app.actors.{ActorTracing, AskSupport, DefaultTimeout}
import io.landy.app.driver.SparkDriverActor
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import spray.can.Http
import spray.can.server.ServerSettings

import scala.concurrent.Await
import scala.concurrent.duration._

class BootActor extends Actor with ActorTracing
                              with AskSupport
                              with DefaultTimeout {

  import App._

  @inline
  private def startStorage(): Unit = {
    import io.landy.app.storage.{Storage, StorageActor}
    val ref: ActorRef = context.actorOf(
      props = Props[StorageActor],
      name = Storage.actorName
    )
  }

  @inline
  private def startAppInstances(): Unit = {
    import io.landy.app.instance.{Mediator, MediatorActor}
    val ref: ActorRef = context.actorOf(
      props = Props[MediatorActor],
      name = Mediator.actorName
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

      val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagerFactory.init(keyStore, password.toCharArray)

      val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      trustManagerFactory.init(keyStore)

      val context = SSLContext.getInstance("TLS")

      context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
      context
    }

    import spray.io.ServerSSLEngineProvider

    implicit lazy val engineProvider = ServerSSLEngineProvider {
      engine => {
        engine.setEnabledProtocols(Array("SSLv3", "TLSv1", "TLSv1.1", "TLSv1.2"))
        engine
      }
    }
  }

  @inline
  private def startHttp() {

    val conf = ConfigFactory.load()

    val privateEndpoint = new {
      val http = new {
        val host = conf.getString ("landy.server.private.http.host")
        val port = conf.getInt    ("landy.server.private.http.port")
      }
    }

    val publicEndpoint = new {
      val http = new {
        val host = conf.getString ("landy.server.public.http.host")
        val port = conf.getInt    ("landy.server.public.http.port")
      }

      val https = new {
        val host = conf.getString ("landy.server.public.https.host")
        val port = conf.getInt    ("landy.server.public.https.port")
      }
    }

    import io.landy.app.endpoints._

    val privateRef: ActorRef = context.actorOf(
      props = Props[PrivateEndpointActor],
      name  = classOf[PrivateEndpointActor].getName
    )

    val publicRef: ActorRef = context.actorOf(
      props = Props[PublicEndpointActor],
      name  = classOf[PublicEndpointActor].getName
    )


    implicit val system: ActorSystem = context.system

    import spray.can.server.ServerSettings

    val settingsHttp = ServerSettings(system).copy(
      // NOTA BENE:
      //  This is required for client-ip resolution
      remoteAddressHeader = true,
      sslEncryption = false
    )

    val settingsHttps = settingsHttp.copy(
      sslEncryption = true
    )

    import spray.can.Http

    //
    // Private endpoint:
    //    > http
    //
    IO(Http) ? bind(
      privateEndpoint.http.host,
      privateEndpoint.http.port,
      privateRef,
      settingsHttp,
      useHttps = false
    )


    //
    // Public endpoint(s):
    //    > http
    //    > https
    //
    IO(Http) ? bind(
      publicEndpoint.http.host,
      publicEndpoint.http.port,
      publicRef,
      settingsHttp,
      useHttps = false
    )

    IO(Http) ? bind(
      publicEndpoint.https.host,
      publicEndpoint.https.port,
      publicRef,
      settingsHttps,
      useHttps = true
    )
  }

  def startSpark() {

    val conf = ConfigFactory.load()

    val sparkConf = new SparkConf() .setMaster    (conf.getString("spark.master"))
                                    .setAppName   (conf.getString("landy.name"))

    val sparkCtx  = new SparkContext(sparkConf)

    context.actorOf(
      props = Props(classOf[SparkDriverActor], sparkCtx),
      name  = classOf[SparkDriverActor].getName
    )
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
      startSpark()
      sender ! true
    }
    case Commands.Shutdown() => {
      context.children.foreach(actor => context.stop(actor))
      sender ! true
    }
  }
}

object App extends DefaultTimeout with AskSupport {

  val appActorName = "app"

  val bootstrapTimeout = Timeout(10.seconds)

  def actor(path: ActorPath)(implicit context: ActorContext): ActorRef =
    Await.result(context.actorSelection(path).resolveOne()(bootstrapTimeout), bootstrapTimeout.duration)

  def actor(path: String)(implicit context: ActorContext): ActorRef =
    actor(context.system / appActorName / path)

  object Commands {
    case class Startup()
    case class Shutdown()
  }

  implicit val system = ActorSystem("landy")

  sys.addShutdownHook { system.shutdown() }

  def main(args: Array[String]): Unit = {
    val bootRef: ActorRef = system.actorOf(Props[BootActor], name = appActorName)

    system.registerOnTermination { bootRef ! Commands.Shutdown() }

    import scala.concurrent.Await

    Await.ready(bootRef ? Commands.Startup(), 60.seconds)

    system.log.info("Started")
  }

}

