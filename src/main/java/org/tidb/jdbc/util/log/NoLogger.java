// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.util.log;

/** No logger implementation */
public class NoLogger implements Logger {

  @Override
  public String getName() {
    return "";
  }

  @Override
  public boolean isTraceEnabled() {
    return false;
  }

  @Override
  public void trace(String msg) {}

  @Override
  public void trace(String format, Object... arguments) {}

  @Override
  public void trace(String msg, Throwable t) {}

  @Override
  public boolean isDebugEnabled() {
    return false;
  }

  @Override
  public void debug(String msg) {}

  @Override
  public void debug(String format, Object... arguments) {}

  @Override
  public void debug(String msg, Throwable t) {}

  @Override
  public boolean isInfoEnabled() {
    return false;
  }

  @Override
  public void info(String msg) {}

  @Override
  public void info(String format, Object... arguments) {}

  @Override
  public void info(String msg, Throwable t) {}

  @Override
  public boolean isWarnEnabled() {
    return false;
  }

  @Override
  public void warn(String msg) {}

  @Override
  public void warn(String format, Object... arguments) {}

  @Override
  public void warn(String msg, Throwable t) {}

  @Override
  public boolean isErrorEnabled() {
    return false;
  }

  @Override
  public void error(String msg) {}

  @Override
  public void error(String format, Object... arguments) {}

  @Override
  public void error(String msg, Throwable t) {}
}
