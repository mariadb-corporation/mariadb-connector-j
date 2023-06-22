// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.util.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.util.log.ConsoleLogger;
import com.singlestore.jdbc.util.log.ConsoleLogger.CONSOLE_LOG_LEVEL;
import com.singlestore.jdbc.util.log.ConsoleLogger.ConsoleLoggerKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class ConsoleLoggerTest {

  @Test
  public void logger() throws IOException {
    logger(CONSOLE_LOG_LEVEL.TRACE);
    logger(CONSOLE_LOG_LEVEL.INFO);
  }

  public void logger(CONSOLE_LOG_LEVEL logLevel) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      try (ByteArrayOutputStream err = new ByteArrayOutputStream()) {

        ConsoleLogger logger =
            new ConsoleLogger(
                new ConsoleLoggerKey("test", logLevel, null),
                new PrintStream(out),
                new PrintStream(err));

        assertEquals("test", logger.getName());
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

        assertEquals(logLevel == CONSOLE_LOG_LEVEL.TRACE, logger.isDebugEnabled());
        logger.debug("debug msg");
        logger.debug("debug msg3 {} {}", 1, "t");
        logger.debug("debug msg2", new SQLException("test"));

        assertEquals(logLevel == CONSOLE_LOG_LEVEL.TRACE, logger.isTraceEnabled());
        logger.trace("trace msg");
        logger.trace("trace msg3 {} {}", 1, "t");
        logger.trace("trace msg2", new SQLException("test"));

        String errSt = err.toString();
        String outSt = out.toString();

        assertTrue(errSt.contains("[main] ERROR test error msg"));
        assertTrue(errSt.contains("[main] ERROR test error msg3 1 t"));
        assertTrue(errSt.contains("[main] ERROR test error msg4 null"));
        assertTrue(errSt.contains("[main] ERROR test null"));
        assertTrue(errSt.contains("[main] ERROR test error msg2 - java.sql.SQLException: test"));
        assertTrue(errSt.contains("[main] WARN test warn msg"));
        assertTrue(errSt.contains("[main] WARN test warn msg3 1 t"));
        assertTrue(errSt.contains("[main] WARN test warn msg2 - java.sql.SQLException: test"));
        assertTrue(outSt.contains("[main] INFO test info msg"));
        assertTrue(outSt.contains("[main] INFO test info msg3 1 t"));
        assertTrue(outSt.contains("[main] INFO test info msg2 - java.sql.SQLException: test"));
        if (logLevel == CONSOLE_LOG_LEVEL.TRACE) {
          assertTrue(outSt.contains("[main] DEBUG test debug msg"));
          assertTrue(outSt.contains("[main] DEBUG test debug msg3 1 t"));
          assertTrue(outSt.contains("[main] DEBUG test debug msg2 - java.sql.SQLException: test"));
          assertTrue(outSt.contains("[main] TRACE test trace msg"));
          assertTrue(outSt.contains("[main] TRACE test trace msg3 1 t"));
          assertTrue(outSt.contains("[main] TRACE test trace msg2 - java.sql.SQLException: test"));
        } else {
          assertFalse(outSt.contains("[DEBUG]"));
          assertFalse(outSt.contains("[TRACE]"));
        }
      }
    }
  }
}
