package io.landy.app

import scala.language.implicitConversions
import scala.util.Random

package object util {

  implicit def boolean2Int(b: Boolean): Int = if (b) 1 else 0
  implicit def arrayOps[T](a: Array[T]): ArrayOps[T] = ArrayOps(a)

  /**
    * Reflection
    */
  object Reflect {
    def moduleFrom[T](sym: scala.reflect.runtime.universe.Symbol): T = {
      val mod = sym.owner.typeSignature.member(sym.name.toTermName)
      reflect.runtime.currentMirror.reflectModule(mod.asModule).instance.asInstanceOf[T]
    }
  }

  /**
    * Functor-specific
    */
  object Identity {
    @inline def partial[T](): PartialFunction[T, T] = { case x => x }
    @inline def total[T](): (T) => T = { x => x }
  }

  /**
    * Some useful array extensions
    */
  case class ArrayOps[T](a: Array[T]) {
    def random: Option[T] =
      if (a.length > 0) Some(a(new Random().nextInt(a.length)))
      else              None
  }
}
