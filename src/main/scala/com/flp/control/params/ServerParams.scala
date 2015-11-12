package com.flp.control.params

import java.net.InetAddress

object ServerParams {

  def get(addr: Option[InetAddress]) =
    Map[String, String](
      "serverTs"  -> java.lang.Long.toHexString(System.currentTimeMillis()),
      "ip"        -> addr.map( a => a.getHostAddress ).getOrElse("")
    )

}
