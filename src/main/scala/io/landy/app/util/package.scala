package io.landy.app

import scala.language.implicitConversions
import scala.util.Random

package object util {

  implicit def boolean2Int(b: Boolean): Int = if (b) 1 else 0

  implicit def anyRef2ElvisOps[T <: AnyRef](o: T): ElvisOps[T] = ElvisOps(o)

  implicit def arrayOps[T](a: Array[T]): ArrayOps[T] = ArrayOps(a)


  /**
    * Random (-string) utilities
    */

  private def randomString(alphabet: String)(length: Int): String = {
    val r = new Random()
    Stream.continually(r.nextInt(alphabet.length)).map(alphabet).take(length).mkString
  }

  def randomHexString(length: Int): String = randomString("abcdefghijklmnopqrstuvwxyz0123456789")(length)

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

  /**
    * Useful extension emulating wide-spread elvis operator (C#, Kotlin, etc.)
    */
  case class ElvisOps[T](one: T) {
    def `??`(other: T) = if (one != null) one else other
  }

}
