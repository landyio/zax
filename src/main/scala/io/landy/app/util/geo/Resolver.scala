package io.landy.app.util.geo

import java.net.InetAddress
import com.maxmind.geoip2.DatabaseReader.Builder
import com.maxmind.geoip2.exception.GeoIp2Exception


object Resolver {

  case class City(name: String, country: String)

  object Unknown extends City("unknown", "unknown")

  val db = new Builder(getClass.getClassLoader.getResourceAsStream("db/geoip/city.mmdb")).build()

  def apply(addr: InetAddress): City =
    try {
      val c = db.city(addr)
      City(c.getCity.getName, c.getCountry.getName)
    } catch {
      case e: GeoIp2Exception => Unknown
    }
}
