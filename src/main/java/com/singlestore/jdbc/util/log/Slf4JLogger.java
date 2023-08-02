// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util.log;

import org.slf4j.Logger;

public class Slf4JLogger implements com.singlestore.jdbc.util.log.Logger {

  private final Logger logger;
  private final boolean printStackTrace;
  private final int maxPrintStackSize;

  public Slf4JLogger(Logger logger, boolean printStackTrace, int maxPrintStackSize) {
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
    return logger.isTraceEnabled();
  }

  @Override
  public void trace(String msg) {
    logger.trace(msg);
    if (printStackTrace()) {
      logger.trace(LoggerHelper.currentStackTrace(maxStackTraceSizeToLog()));
    }
  }

  @Override
  public void trace(String format, Object... arguments) {
    logger.trace(format, arguments);
    if (printStackTrace()) {
      logger.trace(LoggerHelper.currentStackTrace(maxStackTraceSizeToLog()));
    }
  }

  @Override
  public void trace(String msg, Throwable t) {
    logger.trace(msg, t);
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public void debug(String msg) {
    logger.debug(msg);
  }

  @Override
  public void debug(String format, Object... arguments) {
    logger.debug(format, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    logger.debug(msg, t);
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public void info(String msg) {
    logger.info(msg);
  }

  @Override
  public void info(String format, Object... arguments) {
    logger.info(format, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    logger.info(msg, t);
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  @Override
  public void warn(String msg) {
    logger.warn(msg);
  }

  @Override
  public void warn(String format, Object... arguments) {
    logger.warn(format, arguments);
  }

  @Override
  public void warn(String msg, Throwable t) {
    logger.warn(msg, t);
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.isErrorEnabled();
  }

  @Override
  public void error(String msg) {
    logger.error(msg);
  }

  @Override
  public void error(String format, Object... arguments) {
    logger.error(format, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    logger.error(msg, t);
  }
}
