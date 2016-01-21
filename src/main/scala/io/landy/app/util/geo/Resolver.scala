package io.landy.app.util.geo

import java.net.InetAddress
import com.maxmind.geoip2.DatabaseReader.Builder
import com.maxmind.geoip2.exception.GeoIp2Exception

import io.landy.app.util.anyRef2ElvisOps

object Resolver {

  case class City(name: String, country: String)

  val failureDescriptor: String = "<unknown>"

  object Unknown extends City(failureDescriptor, failureDescriptor)

  val db = new Builder(getClass.getClassLoader.getResourceAsStream("db/geoip/city.mmdb")).build()

  def apply(addr: InetAddress): City =
    try {
      val c = db.city(addr)
      City(c.getCity.getName ?? failureDescriptor, c.getCountry.getName ?? failureDescriptor)
    } catch {
      case e: GeoIp2Exception => Unknown
    }
}
