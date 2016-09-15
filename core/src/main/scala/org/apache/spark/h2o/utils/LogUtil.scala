package org.apache.spark.h2o.utils

import org.apache.log4j.{Level, LogManager}
import water.MRTask
import water.util.Log

/**
  * A simple helper to control H2O log subsystem
  */
object LogUtil extends org.apache.spark.internal.Logging {

  def setH2OClientLogLevel(level: String): Unit = {
    setLogLevel(
      level,
      () => logWarning(s"[$level] is not a supported log level."),
      () => logInfo(s"Log level changed to [$level]."))
  }

  def setH2ONodeLogLevel(level: String): Unit = {
    new MRTask() {
      override def setupLocal() {
        setLogLevel(
          level,
          () => Log.warn(s"[$level] is not a supported log level."),
          () => Log.info(s"Log level changed to [$level]."))
      }
    }.doAllNodes()
  }

  private def setLogLevel(level: String, logWarn: () => Unit, logChanged: () => Unit) = {
    val levelIdx = Log.valueOf(level)
    if (levelIdx < 0) {
      logWarn()
    } else {
      LogManager.getLogger("water.default").setLevel(Level.toLevel(level))
      // FIXME: Log._level = levelIdx
      logChanged()
    }
  }
}

