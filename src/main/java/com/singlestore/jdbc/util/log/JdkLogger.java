// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.util.log;

import java.util.logging.Level;
import java.util.regex.Matcher;

public class JdkLogger implements Logger {

  private final java.util.logging.Logger logger;
  private final boolean printStackTrace;
  private final int maxPrintStackSize;

  public JdkLogger(
      java.util.logging.Logger logger, boolean printStackTrace, int maxPrintStackSize) {
    this.logger = logger;
    this.printStackTrace = printStackTrace;
    this.maxPrintStackSize = maxPrintStackSize;
  }

  @Override
  public boolean printStackTrace() {
    return this.printStackTrace;
  }

  @Override
  public int maxStackTraceSizeToLog() {
    return this.maxPrintStackSize;
  }

  @Override
  public String getName() {
    return logger.getName();
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.isLoggable(Level.FINEST);
  }

  @Override
  public void trace(String msg) {
    logger.log(Level.FINEST, msg);
    if (printStackTrace()) {
      logger.log(Level.FINEST, LoggerHelper.currentStackTrace(maxStackTraceSizeToLog()));
    }
  }

  @Override
  public void trace(String format, Object... arguments) {
    logger.log(Level.FINEST, format(format, arguments));
    if (printStackTrace()) {
      logger.log(Level.FINEST, LoggerHelper.currentStackTrace(maxStackTraceSizeToLog()));
    }
  }

  @Override
  public void trace(String msg, Throwable t) {
    logger.log(Level.FINEST, msg, t);
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isLoggable(Level.FINE);
  }

  @Override
  public void debug(String msg) {
    logger.log(Level.FINE, msg);
  }

  @Override
  public void debug(String format, Object... arguments) {
    logger.log(Level.FINE, format(format, arguments));
  }

  @Override
  public void debug(String msg, Throwable t) {
    logger.log(Level.FINE, msg, t);
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isLoggable(Level.INFO);
  }

  @Override
  public void info(String msg) {
    logger.log(Level.INFO, msg);
  }

  @Override
  public void info(String format, Object... arguments) {
    logger.log(Level.INFO, format(format, arguments));
  }

  @Override
  public void info(String msg, Throwable t) {
    logger.log(Level.INFO, msg, t);
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isLoggable(Level.WARNING);
  }

  @Override
  public void warn(String msg) {
    logger.log(Level.WARNING, msg);
  }

  @Override
  public void warn(String format, Object... arguments) {
    logger.log(Level.WARNING, format(format, arguments));
  }

  @Override
  public void warn(String msg, Throwable t) {
    logger.log(Level.WARNING, msg, t);
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.isLoggable(Level.SEVERE);
  }

  @Override
  public void error(String msg) {
    logger.log(Level.SEVERE, msg);
  }

  @Override
  public void error(String format, Object... arguments) {
    logger.log(Level.SEVERE, format(format, arguments));
  }

  @Override
  public void error(String msg, Throwable t) {
    logger.log(Level.SEVERE, msg, t);
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
}
