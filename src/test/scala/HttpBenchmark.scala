import akka.actor.{Props, ActorRef, ActorSystem}
import com.flp.control.akka.DefaultTimeout

import org.scalameter._


object HttpBenchmark /*extends PerformanceTest.Quickbenchmark with DefaultTimeout*/ {
/*
  val system = ActorSystem("flp")
  val applicationRef: ActorRef = system.actorOf(Props[BootActor], name = "application")
  Await.ready(applicationRef ? Boot.Commands.Startup(), 60.seconds)

  val sizes = Gen.range("size")(300000, 1500000, 300000)

  val ranges = for {
    size <- sizes
  } yield size


  performance of "Http" in {
    measure method "predict" in {
      using(ranges) in {
        r =>
      }
    }
  }
*/
}
