package io.landy.app.util

import akka.event.LoggingAdapter

import scala.language.implicitConversions

object Logging {

  case class LoggingAdapterOps(log: LoggingAdapter) {
    /**
      * 'X' stands for 'extend'
      */
    def x = this

    def debug   (template: String, args: Any*) = if (log.isDebugEnabled)    log.debug   (log.format(template, args))
    def info    (template: String, args: Any*) = if (log.isInfoEnabled)     log.info    (log.format(template, args))
    def error   (template: String, args: Any*) = if (log.isErrorEnabled)    log.error   (log.format(template, args))
    def warning (template: String, args: Any*) = if (log.isWarningEnabled)  log.warning (log.format(template, args))

    def error(cause: Throwable, template: String, args: Any*) = if (log.isErrorEnabled) log.error(cause, log.format(template, args))
  }

  implicit def extendLogging(la: LoggingAdapter): LoggingAdapterOps = LoggingAdapterOps(la)

}
