package com.flp.control.model


trait Goal {
  type Type = Boolean
}

case class UserIdentity(params: UserIdentity.Params) {
  def ++(ps: UserIdentity.Params) = UserIdentity(params ++ ps)
}

object UserIdentity {
  type Params       = Map[String, String]
  type Descriptors  = Seq[String]

  val empty = new UserIdentity(Map())
}

case class Variation(id: Variation.Id)

object Variation {
  type Id = String

  val sentinel = new Variation("-1")
}