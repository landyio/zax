package io.landy.app.util

import akka.event.LoggingAdapter

import scala.language.implicitConversions

object Logging {

  case class ExtendedLoggingAdapter(log: LoggingAdapter) extends LoggingAdapter {
    /**
      * 'X' stands for 'extend'
      */
    def x = this

    override def isErrorEnabled   = log.isErrorEnabled
    override def isInfoEnabled    = log.isInfoEnabled
    override def isDebugEnabled   = log.isDebugEnabled
    override def isWarningEnabled = log.isWarningEnabled

    def debug   (template: String, args: Any*) = if (isDebugEnabled) notifyDebug(format(template, args))
    def info    (template: String, args: Any*) = if (isInfoEnabled) notifyInfo(format(template, args))
    def error   (template: String, args: Any*) = if (isErrorEnabled) notifyError(format(template, args))
    def warning (template: String, args: Any*) = if (isWarningEnabled) notifyWarning(format(template, args))

    def error(cause: Throwable, template: String, args: Any*) = if (isErrorEnabled) notifyError(cause, format(template, args))

    override protected def notifyInfo(message: String) = log.notifyInfo(message)

    override protected def notifyError(message: String) = log.notifyError(message)

    override protected def notifyError(cause: Throwable, message: String) = log.notifyError(cause, message)

    override protected def notifyWarning(message: String) = log.notifyWarning(message)

    override protected def notifyDebug(message: String) = log.notifyDebug(message)
  }

  implicit def extendLogging(la: LoggingAdapter): ExtendedLoggingAdapter = ExtendedLoggingAdapter(la)

}
