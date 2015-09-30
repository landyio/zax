package com.flp.control.akka

import akka.util.Timeout

trait DefaultTimeout {
  import scala.concurrent.duration._
  implicit val timeout: Timeout = (2.seconds)
}
