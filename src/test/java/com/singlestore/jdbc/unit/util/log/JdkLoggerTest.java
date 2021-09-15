// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.unit.util.log;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.util.log.JdkLogger;
import java.io.*;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.logging.*;
import org.junit.jupiter.api.Test;

public class JdkLoggerTest {
  ByteArrayOutputStream out = new ByteArrayOutputStream();

  public class BufHandler extends StreamHandler {
    public BufHandler() {
      super(out, new MySimpleFormatter());
    }

    public void publish(LogRecord record) {
      super.publish(record);
      this.flush();
    }

    public void close() {
      this.flush();
    }
  }

  public class MySimpleFormatter extends Formatter {
    private final String format = "[%4$-7s] %5$s %n";

    public MySimpleFormatter() {}

    public String format(LogRecord record) {
      ZonedDateTime zdt = ZonedDateTime.now();
      String source;
      if (record.getSourceClassName() != null) {
        source = record.getSourceClassName();
        if (record.getSourceMethodName() != null) {
          source = source + " " + record.getSourceMethodName();
        }
      } else {
        source = record.getLoggerName();
      }

      String message = this.formatMessage(record);
      String throwable = "";
      if (record.getThrown() != null) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println();
        record.getThrown().printStackTrace(pw);
        pw.close();
        throwable = sw.toString();
      }

      return String.format(
          this.format, zdt, source, record.getLoggerName(), Level.FINEST, message, throwable);
    }
  }

  @Test
  public void logger() throws IOException {

    java.util.logging.Logger log = Logger.getLogger("logger");
    log.addHandler(new BufHandler());
    log.setLevel(Level.FINEST);
    JdkLogger logger = new JdkLogger(log);

    assertEquals("logger", logger.getName());
    assertTrue(logger.isErrorEnabled());
    logger.error("error msg");
    logger.error("error msg3 {} {}", 1, "t");
    logger.error("error msg4 {}", (String) null);
    logger.error(null, (String) null);
    logger.error("error msg2", new SQLException("test"));

    assertTrue(logger.isInfoEnabled());
    logger.info("info msg");
    logger.info("info msg3 {} {}", 1, "t");
    logger.info("info msg2", new SQLException("test"));

    assertTrue(logger.isWarnEnabled());
    logger.warn("warn msg");
    logger.warn("warn msg3 {} {}", 1, "t");
    logger.warn("warn msg2", new SQLException("test"));

    assertTrue(logger.isDebugEnabled());
    logger.debug("debug msg");
    logger.debug("debug msg3 {} {}", 1, "t");
    logger.debug("debug msg2", new SQLException("test"));

    assertTrue(logger.isTraceEnabled());
    logger.trace("trace msg");
    logger.trace("trace msg3 {} {}", 1, "t");
    logger.trace("trace msg3 {} {}", (String) null);
    logger.trace("trace msg2", new SQLException("test"));

    String outSt = new String(out.toByteArray());
    String expected =
        "[FINEST ] error msg \n"
            + "[FINEST ] error msg3 1 t \n"
            + "[FINEST ] error msg4 null \n"
            + "[FINEST ] null \n"
            + "[FINEST ] error msg2 \n"
            + "[FINEST ] info msg \n"
            + "[FINEST ] info msg3 1 t \n"
            + "[FINEST ] info msg2 \n"
            + "[FINEST ] warn msg \n"
            + "[FINEST ] warn msg3 1 t \n"
            + "[FINEST ] warn msg2 ";
    assertTrue(outSt.contains(expected) || outSt.replace("\r\n", "\n").contains(expected));
  }
}
