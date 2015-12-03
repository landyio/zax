package io.landy.app.model


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
  * @param value unique id of the variation
  */
case class Variation(id: Variation.Id, value: Variation.Type)

object Variation {
  type Type = String

  /**
    * NOTA BENE
    * That's here primarily to hedge implicit conversions of the `String` to `BSONString`
    */
  case class Id(value: String)

  val `value` = "value"
  val `id`    = "id"
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