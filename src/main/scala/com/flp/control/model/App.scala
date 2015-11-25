package com.flp.control.model


/**
  * Goal
  */
trait Goal {
  type Type = Boolean
}


/**
  * User identity
  *
  * @param params parameters specifiying particular user-identity
  */
case class UserIdentity(params: UserIdentity.Params) {
  def ++(ps: UserIdentity.Params) = UserIdentity(params ++ ps)

  def toFeatures(descriptors: Seq[UserDataDescriptor]): Vector[Double] =
    descriptors .map { d => d.hash(params.get(d.name)).toDouble }
                .toVector
}

object UserIdentity {
  type Params       = Map[String, String]
  type Descriptors  = Seq[String]

  val empty = new UserIdentity(Map())
}

/**
  * Variation
  *
  * @param id unique id of the variation
  */
case class Variation(id: Variation.Id)

object Variation {
  type Id = String

  val sentinel = new Variation("-1")
}


/**
  * User's identity detail descriptor
  *
  * @param name name of the detail
  * @param hash optional hash-function
  */
case class UserDataDescriptor(
  name:         String,
  categorical:  Boolean,
  hash:         (Option[String] => Int) = UserDataDescriptor.defaultHash
)

object UserDataDescriptor {
  val `name` = "name"
  val `categorical` = "categorical"

  protected val defaultHash: Option[String] => Int = o => o.map { _.hashCode() }.getOrElse(0)
}