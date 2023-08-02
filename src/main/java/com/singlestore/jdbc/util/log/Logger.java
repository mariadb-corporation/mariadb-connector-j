// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util.log;

public interface Logger {

  boolean printStackTrace();

  int maxStackTraceSizeToLog();

  String getName();

  boolean isTraceEnabled();

  void trace(String msg);

  void trace(String format, Object... arguments);

  void trace(String msg, Throwable t);

  boolean isDebugEnabled();

  void debug(String msg);

  void debug(String format, Object... arguments);

  void debug(String msg, Throwable t);

  boolean isInfoEnabled();

  void info(String msg);

  void info(String format, Object... arguments);

  void info(String msg, Throwable t);

  boolean isWarnEnabled();

  void warn(String msg);

  void warn(String format, Object... arguments);

  void warn(String msg, Throwable t);

  boolean isErrorEnabled();

  void error(String msg);

  void error(String format, Object... arguments);

  void error(String msg, Throwable t);
}
