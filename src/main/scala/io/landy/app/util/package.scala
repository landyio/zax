package io.landy.app

import scala.language.implicitConversions

package object util {

  implicit def boolean2Int(b: Boolean): Int = if (b) 1 else 0

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
}
