package com.flp.control

import scala.language.implicitConversions

package object util {

  implicit def boolean2Int(b: Boolean): Int = if (b) 1 else 0

  object Identity {
    @inline def partial[T](): PartialFunction[T, T] = { case x => x }
    @inline def total[T](): (T) => T = { x => x }
  }
}
