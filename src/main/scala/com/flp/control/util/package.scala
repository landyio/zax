package com.flp.control

package object util {

  implicit def boolean2Int(b: Boolean): Int = if (b) 1 else 0

}
