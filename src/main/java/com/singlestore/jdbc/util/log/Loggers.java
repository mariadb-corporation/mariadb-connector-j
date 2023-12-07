// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.util.log;

import com.singlestore.jdbc.util.log.ConsoleLogger.CONSOLE_LOG_LEVEL;
import com.singlestore.jdbc.util.log.ConsoleLogger.ConsoleLoggerKey;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Function;

public final class Loggers {

  private static Boolean slf4jEnabled;
  private static CONSOLE_LOG_LEVEL consoleLogLevel;
  private static boolean printStackTrace;
  private static int maxPrintStackSizeToLog;
  private static String consoleLogFilepath;
  private static PrintStream filePrintStream;
  public static final String FALLBACK_PROPERTY = "singlestore.logging.fallback";
  public static final String CONSOLE_DEBUG_PROPERTY = "singlestore.logging.fallback.console.debug";
  public static final String TEST_ENABLE_SLF4J = "singlestore.logging.slf4j.enable";
  private static Function<String, ? extends Logger> LOGGER_FACTORY;

  static {
    resetLoggerFactory();
  }

  public static Logger getLogger(String name) {
    return LOGGER_FACTORY.apply(name);
  }

  public static Logger getLogger(Class<?> cls) {
    return LOGGER_FACTORY.apply(cls.getName());
  }

  private static class Slf4JLoggerFactory implements Function<String, Logger> {

    private static final Map<String, WeakReference<Logger>> slf4jLoggers = new WeakHashMap<>();

    @Override
    public Logger apply(String name) {
      synchronized (slf4jLoggers) {
        final WeakReference<Logger> ref = slf4jLoggers.get(name);
        Logger cached = ref == null ? null : ref.get();
        if (cached == null) {
          cached =
              new Slf4JLogger(
                  org.slf4j.LoggerFactory.getLogger(name), printStackTrace, maxPrintStackSizeToLog);
          slf4jLoggers.put(name, new WeakReference<>(cached));
        }
        return cached;
      }
    }
  }

  /** JDK wrapper */
  private static class JdkLoggerFactory implements Function<String, Logger> {

    private static final Map<String, WeakReference<Logger>> jdkLoggers = new WeakHashMap<>();

    @Override
    public Logger apply(String name) {
      synchronized (jdkLoggers) {
        final WeakReference<Logger> ref = jdkLoggers.get(name);
        Logger cached = ref == null ? null : ref.get();
        if (cached == null) {
          cached =
              new JdkLogger(
                  java.util.logging.Logger.getLogger(name),
                  printStackTrace,
                  maxPrintStackSizeToLog);
          jdkLoggers.put(name, new WeakReference<>(cached));
        }
        return cached;
      }
    }
  }

  private static final class ConsoleLoggerFactory implements Function<String, Logger> {

    private static final Map<ConsoleLoggerKey, WeakReference<Logger>> consoleLoggers =
        new WeakHashMap<>();

    @Override
    public Logger apply(String name) {
      final ConsoleLoggerKey key =
          new ConsoleLoggerKey(
              name,
              getConsoleLogLevel(),
              getConsoleLogFilepath(),
              printStackTrace,
              maxPrintStackSizeToLog);
      synchronized (consoleLoggers) {
        final WeakReference<Logger> ref = consoleLoggers.get(key);
        Logger cached = ref == null ? null : ref.get();
        if (cached == null) {
          cached = new ConsoleLogger(key, filePrintStream, filePrintStream);
          consoleLoggers.put(key, new WeakReference<>(cached));
        }
        return cached;
      }
    }
  }

  public static void resetLoggerFactoryProperties(
      String level, String path, boolean printTrace, int maxPrintStackSize) {
    synchronized (Loggers.class) {
      printStackTrace = printTrace;
      maxPrintStackSizeToLog = maxPrintStackSize;
      slf4jEnabled = level == null && path == null;
      consoleLogLevel = CONSOLE_LOG_LEVEL.fromLevelName(level);
      String error = null;
      if (path != null && path.length() > 0 && !path.equals(consoleLogFilepath)) {
        consoleLogFilepath = path;
        try {
          filePrintStream = new PrintStream(new FileOutputStream(consoleLogFilepath, true), true);
        } catch (FileNotFoundException e) {
          error = String.format("failed to create log file stream, error: %s", e.getMessage());
        }
      }
      resetLoggerFactory();
      if (error != null) {
        getLogger(Loggers.class).error(error);
      }
    }
  }

  /**
   * Return true if {@link #resetLoggerFactory()} would fallback to java.util.logging rather than
   * console-based logging, as defined by the {@link #FALLBACK_PROPERTY} System property.
   *
   * @return true if falling back to JDK, false for Console.
   */
  static boolean isFallbackToJdk() {
    return "JDK".equalsIgnoreCase(System.getProperty(FALLBACK_PROPERTY));
  }

  static boolean isSlf4jEnabled() {
    return slf4jEnabled == null
        ? Boolean.parseBoolean(System.getProperty(TEST_ENABLE_SLF4J, "true"))
        : slf4jEnabled;
  }

  static CONSOLE_LOG_LEVEL getConsoleLogLevel() {
    if (consoleLogLevel != null) {
      return consoleLogLevel;
    } else {
      return Boolean.parseBoolean(System.getProperty(CONSOLE_DEBUG_PROPERTY, "false"))
          ? CONSOLE_LOG_LEVEL.TRACE
          : CONSOLE_LOG_LEVEL.INFO;
    }
  }

  public static String getConsoleLogFilepath() {
    return consoleLogFilepath;
  }

  private static void resetLoggerFactory() {
    synchronized (Loggers.class) {
      Function<String, Logger> loggerFactory = null;
      try {
        if (isSlf4jEnabled()) {
          Class.forName("org.slf4j.LoggerFactory");
          loggerFactory = new Slf4JLoggerFactory();
        }
      } catch (ClassNotFoundException cle) {
        // slf4j not in the classpath
      }
      if (loggerFactory == null) {
        // default to console or use JDK logger if explicitly set by System property
        if (isFallbackToJdk()) {
          loggerFactory = new JdkLoggerFactory();
        } else {
          loggerFactory = new ConsoleLoggerFactory();
        }
      }
      LOGGER_FACTORY = loggerFactory;
    }
  }
}
