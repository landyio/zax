package com.flp.control.instance.util

import scala.language.implicitConversions

object State {

  /**
    * Placeholder designating predicate as universaly true (ie that state
    * could be of any value)
    */
  object Any

  /**
    * Abstract composable matcher
    * @tparam S type of state to be matched against
    */
  trait Matcher[S] {
    def or(s: S): Matcher[S]
    def then[R](trigger: => R): Unit
  }

  case class AnyMatcher() extends Matcher[Any.type] {
    def or(s: Any.type) = this
    override def then[R](run: => R) = run
  }

  /**
    * Dispatcher holding current-value of `state` to be matched against
    *
    * @param current value of given state
    * @tparam S type of state to be matched against
    */
  case class StateDispatcher[S](current: S) {
    type Value = Either[S, Any.type]

    case class MultiplexingMatcher[S](allowedStates: Seq[S]) extends Matcher[S] {
      def or(state: S): Matcher[S] =
        MultiplexingMatcher(allowedStates :+ state)

      def then[R](run: => R) = {
        if (allowedStates.contains(current)) run
      }
    }

    def is(state: Any.type)  = AnyMatcher()
    def is(state: S)              = MultiplexingMatcher(Seq(state))
  }

  implicit def respondIf[S](state: S): StateDispatcher[S] =
    StateDispatcher(state)
}