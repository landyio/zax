package com.flp.control.instance

trait UserDataDescriptor {
  val name: String
  val hash: (Option[String] => Double)
}

object UserDataDescriptor {
  private val default = (o: Option[String]) => o.map { _.hashCode() % 16 }.getOrElse(0).toDouble
  def apply(name: String, hash: (Option[String] => Double) = default) =
    new UserDataDescriptorImpl(name, hash)
}

class UserDataDescriptorImpl(
  override val name: String,
  override val hash: (Option[String] => Double)
) extends UserDataDescriptor

