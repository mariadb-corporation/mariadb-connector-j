// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.util.log;

/** Logger interface */
public interface Logger {
  /**
   * Logger name
   *
   * @return logger name
   */
  String getName();

  /**
   * Must TRACE level be logged
   *
   * @return if TRACE level be logger
   */
  boolean isTraceEnabled();

  /**
   * Log a message at the TRACE level.
   *
   * @param msg the message string to be logged
   */
  void trace(String msg);

  /**
   * Log a message at the TRACE level.
   *
   * @param format the format string
   * @param arguments arguments
   */
  void trace(String format, Object... arguments);

  /**
   * Log a message with an error at the TRACE level.
   *
   * @param msg message
   * @param t error
   */
  void trace(String msg, Throwable t);

  /**
   * must DEBUG level message be logged
   *
   * @return are DEBUG message to be logged
   */
  boolean isDebugEnabled();

  /**
   * Log a message at the DEBUG level.
   *
   * @param msg the message string to be logged
   */
  void debug(String msg);

  /**
   * Log a message at the DEBUG level.
   *
   * @param format the format string
   * @param arguments arguments
   */
  void debug(String format, Object... arguments);

  /**
   * Log a message with an error at the DEBUG level.
   *
   * @param msg message
   * @param t error
   */
  void debug(String msg, Throwable t);

  /**
   * Must INFO level be logged
   *
   * @return if INFO level be logger
   */
  boolean isInfoEnabled();

  /**
   * Log a message at the INFO level.
   *
   * @param msg the message string to be logged
   */
  void info(String msg);

  /**
   * Log a message at the INFO level.
   *
   * @param format the format string
   * @param arguments arguments
   */
  void info(String format, Object... arguments);

  /**
   * Log a message with an error at the INFO level.
   *
   * @param msg message
   * @param t error
   */
  void info(String msg, Throwable t);

  /**
   * Must WARN level be logged
   *
   * @return if WARN level be logger
   */
  boolean isWarnEnabled();

  /**
   * Log a message at the WARN level.
   *
   * @param msg the message string to be logged
   */
  void warn(String msg);

  /**
   * Log a message at the WARNING level.
   *
   * @param format the format string
   * @param arguments arguments
   */
  void warn(String format, Object... arguments);

  /**
   * Log a message with an error at the WARNING level.
   *
   * @param msg message
   * @param t error
   */
  void warn(String msg, Throwable t);

  /**
   * Must ERROR level be logged
   *
   * @return if ERROR level be logger
   */
  boolean isErrorEnabled();

  /**
   * Log a message at the ERROR level.
   *
   * @param msg the message string to be logged
   */
  void error(String msg);

  /**
   * Log a message at the ERROR level.
   *
   * @param format the format string
   * @param arguments arguments
   */
  void error(String format, Object... arguments);

  /**
   * Log a message with an error at the ERROR level.
   *
   * @param msg message
   * @param t error
   */
  void error(String msg, Throwable t);
}
