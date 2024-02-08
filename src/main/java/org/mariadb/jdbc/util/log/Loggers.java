// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.util.log;

import java.util.HashMap;

/** Loggers factory */
public final class Loggers {

  /** defined if using JDK if sfl4j is not present. possible values: JDK/CONSOLE */
  public static final String FALLBACK_PROPERTY = "mariadb.logging.fallback";

  /** set if console must log debug level */
  public static final String CONSOLE_DEBUG_PROPERTY = "mariadb.logging.fallback.console.debug";

  /** !! testing only !! permit to disable SLF4J implementation even if SLF4j is present */
  public static final String TEST_ENABLE_SLF4J = "mariadb.logging.slf4j.enable";

  /** disable logging */
  public static final String NO_LOGGER_PROPERTY = "mariadb.logging.disable";

  /** factory */
  private static LoggerFactory LOGGER_FACTORY;

  static {
    init();
  }

  /**
   * Return default logger implementation
   *
   * @param name logger name
   * @return logger implementation
   */
  public static Logger getLogger(String name) {
    return LOGGER_FACTORY.getLogger(name);
  }

  /**
   * Return default logger implementation
   *
   * @param cls class
   * @return logger implementation
   */
  public static Logger getLogger(Class<?> cls) {
    return LOGGER_FACTORY.getLogger(cls.getName());
  }

  /** Initialize factory */
  public static void init() {
    String name = LoggerFactory.class.getName();
    LoggerFactory loggerFactory = null;
    if (Boolean.parseBoolean(System.getProperty(NO_LOGGER_PROPERTY, "false"))) {
      loggerFactory = new NoLoggerFactory();
    } else {

      try {
        if (Boolean.parseBoolean(System.getProperty(TEST_ENABLE_SLF4J, "true"))) {
          Class.forName("org.slf4j.LoggerFactory");
          loggerFactory = new Slf4JLoggerFactory();
        }
      } catch (ClassNotFoundException cle) {
        // slf4j not in the classpath
      }
      if (loggerFactory == null) {
        // default to console or use JDK logger if explicitly set by System property
        if ("JDK".equalsIgnoreCase(System.getProperty(FALLBACK_PROPERTY))) {
          loggerFactory = new JdkLoggerFactory();
        } else {
          loggerFactory = new ConsoleLoggerFactory();
        }
      }
      try {
        loggerFactory.getLogger(name);
      } catch (Throwable e) {
        // eat
      }
    }
    LOGGER_FACTORY = loggerFactory;
  }

  private interface LoggerFactory {
    Logger getLogger(String name);
  }

  private static class NoLoggerFactory implements LoggerFactory {
    @Override
    public Logger getLogger(String name) {
      return new NoLogger();
    }
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
