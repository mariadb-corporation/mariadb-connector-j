// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util.log;

import java.io.PrintStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;

@SuppressWarnings("ALL")
public class ConsoleLogger implements Logger {

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ");
  private ConsoleLoggerKey identifier;
  private final PrintStream err;
  private final PrintStream log;

  public ConsoleLogger(ConsoleLoggerKey identifier, PrintStream log, PrintStream err) {
    this.identifier = identifier;
    this.log = log == null ? System.out : log;
    this.err = err == null ? System.err : err;
  }

  public ConsoleLogger(ConsoleLoggerKey identifier) {
    this(identifier, System.out, System.err);
  }

  @Override
  public boolean printStackTrace() {
    return this.identifier.isPrintStackTrace();
  }

  @Override
  public int maxStackTraceSizeToLog() {
    return this.identifier.maxPrintStackSizeToLog();
  }

  @Override
  public String getName() {
    return this.identifier.name;
  }

  final String format(String from, Object... arguments) {
    if (from != null) {
      String computed = from;
      for (Object argument : arguments) {
        computed =
            computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(String.valueOf(argument)));
      }
      return computed;
    }
    return null;
  }

  @Override
  public boolean isTraceEnabled() {
    return identifier.logLevel.getLevel() >= CONSOLE_LOG_LEVEL.TRACE.getLevel();
  }

  @Override
  public synchronized void trace(String msg) {
    if (!isTraceEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] TRACE %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg);
    if (printStackTrace()) {
      log.format(
          "%s [%s] TRACE %s %s\n",
          currentTimestamp(),
          Thread.currentThread().getName(),
          getName(),
          LoggerHelper.currentStackTrace(maxStackTraceSizeToLog()));
    }
  }

  @Override
  public synchronized void trace(String format, Object... arguments) {
    if (!isTraceEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] TRACE %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), format(format, arguments));
    if (printStackTrace()) {
      log.format(
          "%s [%s] TRACE %s %s\n",
          currentTimestamp(),
          Thread.currentThread().getName(),
          getName(),
          LoggerHelper.currentStackTrace(maxStackTraceSizeToLog()));
    }
  }

  @Override
  public synchronized void trace(String msg, Throwable t) {
    if (!isTraceEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] TRACE %s %s - %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg, t);
    t.printStackTrace(this.log);
  }

  @Override
  public boolean isDebugEnabled() {
    return this.identifier.logLevel.getLevel() >= CONSOLE_LOG_LEVEL.DEBUG.getLevel();
  }

  @Override
  public synchronized void debug(String msg) {
    if (!isDebugEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] DEBUG %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg);
  }

  @Override
  public synchronized void debug(String format, Object... arguments) {
    if (!isDebugEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] DEBUG %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), format(format, arguments));
  }

  @Override
  public synchronized void debug(String msg, Throwable t) {
    if (!isDebugEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] DEBUG %s %s - %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg, t);
    t.printStackTrace(this.log);
  }

  @Override
  public boolean isInfoEnabled() {
    return this.identifier.logLevel.getLevel() >= CONSOLE_LOG_LEVEL.INFO.getLevel();
  }

  @Override
  public synchronized void info(String msg) {
    if (!isInfoEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] INFO %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg);
  }

  @Override
  public synchronized void info(String format, Object... arguments) {
    if (!isInfoEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] INFO %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), format(format, arguments));
  }

  @Override
  public synchronized void info(String msg, Throwable t) {
    if (!isInfoEnabled()) {
      return;
    }
    this.log.format(
        "%s [%s] INFO %s %s - %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg, t);
    t.printStackTrace(this.log);
  }

  @Override
  public boolean isWarnEnabled() {
    return this.identifier.logLevel.getLevel() >= CONSOLE_LOG_LEVEL.WARN.getLevel();
  }

  @Override
  public synchronized void warn(String msg) {
    if (!isWarnEnabled()) {
      return;
    }
    this.err.format(
        "%s [%s] WARN %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg);
  }

  @Override
  public synchronized void warn(String format, Object... arguments) {
    if (!isWarnEnabled()) {
      return;
    }
    this.err.format(
        "%s [%s] WARN %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), format(format, arguments));
  }

  @Override
  public synchronized void warn(String msg, Throwable t) {
    if (!isWarnEnabled()) {
      return;
    }
    this.err.format(
        "%s [%s] WARN %s %s - %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg, t);
    t.printStackTrace(this.err);
  }

  @Override
  public boolean isErrorEnabled() {
    return this.identifier.logLevel.getLevel() >= CONSOLE_LOG_LEVEL.ERROR.getLevel();
  }

  @Override
  public synchronized void error(String msg) {
    if (!isErrorEnabled()) {
      return;
    }
    this.err.format(
        "%s [%s] ERROR %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg);
  }

  @Override
  public synchronized void error(String format, Object... arguments) {
    if (!isErrorEnabled()) {
      return;
    }
    this.err.format(
        "%s [%s] ERROR %s %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), format(format, arguments));
  }

  @Override
  public synchronized void error(String msg, Throwable t) {
    if (!isErrorEnabled()) {
      return;
    }
    this.err.format(
        "%s [%s] ERROR %s %s - %s\n",
        currentTimestamp(), Thread.currentThread().getName(), getName(), msg, t);
    t.printStackTrace(this.err);
  }

  private static String currentTimestamp() {
    ZonedDateTime zdt = ZonedDateTime.now();
    return zdt.format(FORMATTER);
  }

  public static enum CONSOLE_LOG_LEVEL {
    ERROR(0),
    WARN(1),
    INFO(2),
    DEBUG(3),
    TRACE(4);

    private int level;

    CONSOLE_LOG_LEVEL(int level) {
      this.level = level;
    }

    public int getLevel() {
      return level;
    }

    public static CONSOLE_LOG_LEVEL fromLevelName(String name) {
      return Arrays.stream(values())
          .filter(v -> v.name().equalsIgnoreCase(name))
          .findFirst()
          .orElse(null);
    }
  }

  public static final class ConsoleLoggerKey {

    private final String name;
    private final CONSOLE_LOG_LEVEL logLevel;
    private final String logFilePath;
    private final boolean printStackTrace;
    private final int maxPrintStackSizeToLog;

    public ConsoleLoggerKey(
        String name,
        CONSOLE_LOG_LEVEL logLevel,
        String logFilePath,
        boolean printStackTrace,
        int maxPrintStackSizeToLog) {
      this.name = name;
      this.logLevel = logLevel;
      this.logFilePath = logFilePath;
      this.printStackTrace = printStackTrace;
      this.maxPrintStackSizeToLog = maxPrintStackSizeToLog;
    }

    public boolean isPrintStackTrace() {
      return printStackTrace;
    }

    public int maxPrintStackSizeToLog() {
      return maxPrintStackSizeToLog;
    }

    public String getLogFilePath() {
      return logFilePath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ConsoleLoggerKey that = (ConsoleLoggerKey) o;
      return Objects.equals(name, that.name)
          && logLevel == that.logLevel
          && Objects.equals(logFilePath, that.logFilePath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, logLevel, logFilePath);
    }
  }
}
