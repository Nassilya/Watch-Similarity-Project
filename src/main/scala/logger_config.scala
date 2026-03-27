import java.io.OutputStream
import java.util.logging.{Formatter, Level, LogRecord, Logger, StreamHandler}

object logger_config {
  val logger: Logger = {
    val appLogger = Logger.getLogger("WatchesParsing")

    if (appLogger.getHandlers.isEmpty) {
      appLogger.setUseParentHandlers(false)

      val stdoutHandler = new StreamHandler(System.out, new Formatter {
        override def format(record: LogRecord): String = {
          s"${record.getLevel} | ${record.getLoggerName} | ${record.getMessage}${System.lineSeparator()}"
        }
      }) {
        override def publish(record: LogRecord): Unit = {
          super.publish(record)
          flush()
        }

        override def close(): Unit = {
          flush()
        }
      }

      stdoutHandler.setLevel(Level.INFO)
      appLogger.addHandler(stdoutHandler)
    }

    appLogger.setLevel(Level.INFO)
    appLogger
  }
}
