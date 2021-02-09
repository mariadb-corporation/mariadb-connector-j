package org.mariadb.jdbc.util.log;

import java.util.HashMap;

public class Loggers {

  public static final String FALLBACK_PROPERTY = "mariadb.logging.fallback";
  public static final String CONSOLE_DEBUG_PROPERTY = "mariadb.logging.fallback.console.debug";

  private static LoggerFactory LOGGER_FACTORY;

  static {
    String name = LoggerFactory.class.getName();
    LoggerFactory loggerFactory;
    try {
      loggerFactory = new Slf4JLoggerFactory();
      loggerFactory.getLogger(name);
    } catch (Throwable t) {
      // default to console or use JDK logger if explicitly set by System property
      if ("JDK".equalsIgnoreCase(System.getProperty(FALLBACK_PROPERTY))) {
        loggerFactory = new JdkLoggerFactory();
        loggerFactory.getLogger(name);
      } else {
        loggerFactory = new ConsoleLoggerFactory();
        loggerFactory.getLogger(name);
      }
    }
    LOGGER_FACTORY = loggerFactory;
  }

  public static Logger getLogger(String name) {
    return LOGGER_FACTORY.getLogger(name);
  }

  public static Logger getLogger(Class<?> cls) {
    return LOGGER_FACTORY.getLogger(cls.getName());
  }

  private interface LoggerFactory {
    Logger getLogger(String name);
  }

  private static class Slf4JLoggerFactory implements LoggerFactory {
    @Override
    public Logger getLogger(String name) {
      return new Slf4JLogger(org.slf4j.LoggerFactory.getLogger(name));
    }
  }

  /** JDK wrapper */
  private static class JdkLoggerFactory implements LoggerFactory {
    @Override
    public Logger getLogger(String name) {
      return new JdkLogger(java.util.logging.Logger.getLogger(name));
    }
  }

  private static final class ConsoleLoggerFactory implements LoggerFactory {

    private static final HashMap<String, Logger> consoleLoggers = new HashMap<>();

    @Override
    public Logger getLogger(String name) {
      return consoleLoggers.computeIfAbsent(
          name, n -> new ConsoleLogger(n, System.getProperty(CONSOLE_DEBUG_PROPERTY) != null));
    }
  }
}
