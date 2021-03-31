package org.mariadb.jdbc.util.log;

import java.io.PrintStream;
import java.util.regex.Matcher;

public class ConsoleLogger implements Logger {

  private final String name;
  private final PrintStream err;
  private final PrintStream log;
  private final boolean logDebugLvl;

  public ConsoleLogger(String name, PrintStream log, PrintStream err, boolean logDebugLvl) {
    this.name = name;
    this.log = log;
    this.err = err;
    this.logDebugLvl = logDebugLvl;
  }

  ConsoleLogger(String name, boolean logDebugLvl) {
    this(name, System.out, System.err, logDebugLvl);
  }

  @Override
  public String getName() {
    return this.name;
  }

  final String format(String from, Object... arguments) {
    if (from != null) {
      String computed = from;
      if (arguments != null && arguments.length != 0) {
        for (Object argument : arguments) {
          computed =
              computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(String.valueOf(argument)));
        }
      }
      return computed;
    }
    return null;
  }

  @Override
  public boolean isTraceEnabled() {
    return logDebugLvl;
  }

  @Override
  public synchronized void trace(String msg) {
    if (!logDebugLvl) {
      return;
    }
    this.log.format("[TRACE] (%s) %s\n", Thread.currentThread().getName(), msg);
  }

  @Override
  public synchronized void trace(String format, Object... arguments) {
    if (!logDebugLvl) {
      return;
    }
    this.log.format(
        "[TRACE] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
  }

  @Override
  public synchronized void trace(String msg, Throwable t) {
    if (!logDebugLvl) {
      return;
    }
    this.log.format("[TRACE] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
    t.printStackTrace(this.log);
  }

  @Override
  public boolean isDebugEnabled() {
    return logDebugLvl;
  }

  @Override
  public synchronized void debug(String msg) {
    if (!logDebugLvl) {
      return;
    }
    this.log.format("[DEBUG] (%s) %s\n", Thread.currentThread().getName(), msg);
  }

  @Override
  public synchronized void debug(String format, Object... arguments) {
    if (!logDebugLvl) {
      return;
    }
    this.log.format(
        "[DEBUG] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
  }

  @Override
  public synchronized void debug(String msg, Throwable t) {
    if (!logDebugLvl) {
      return;
    }
    this.log.format("[DEBUG] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
    t.printStackTrace(this.log);
  }

  @Override
  public boolean isInfoEnabled() {
    return true;
  }

  @Override
  public synchronized void info(String msg) {
    this.log.format("[ INFO] (%s) %s\n", Thread.currentThread().getName(), msg);
  }

  @Override
  public synchronized void info(String format, Object... arguments) {
    this.log.format(
        "[ INFO] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
  }

  @Override
  public synchronized void info(String msg, Throwable t) {
    this.log.format("[ INFO] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
    t.printStackTrace(this.log);
  }

  @Override
  public boolean isWarnEnabled() {
    return true;
  }

  @Override
  public synchronized void warn(String msg) {
    this.err.format("[ WARN] (%s) %s\n", Thread.currentThread().getName(), msg);
  }

  @Override
  public synchronized void warn(String format, Object... arguments) {
    this.err.format(
        "[ WARN] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
  }

  @Override
  public synchronized void warn(String msg, Throwable t) {
    this.err.format("[ WARN] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
    t.printStackTrace(this.err);
  }

  @Override
  public boolean isErrorEnabled() {
    return true;
  }

  @Override
  public synchronized void error(String msg) {
    this.err.format("[ERROR] (%s) %s\n", Thread.currentThread().getName(), msg);
  }

  @Override
  public synchronized void error(String format, Object... arguments) {
    this.err.format(
        "[ERROR] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
  }

  @Override
  public synchronized void error(String msg, Throwable t) {
    this.err.format("[ERROR] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
    t.printStackTrace(this.err);
  }
}
