// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.util.log;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.log.ConsoleLogger;

public class ConsoleLoggerTest {

  @Test
  public void logger() throws IOException {
    logger(true);
    logger(false);
  }

  public void logger(boolean logDebug) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      try (ByteArrayOutputStream err = new ByteArrayOutputStream()) {

        ConsoleLogger logger =
            new ConsoleLogger("test", new PrintStream(out), new PrintStream(err), logDebug);

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

        assertEquals(logDebug, logger.isDebugEnabled());
        logger.debug("debug msg");
        logger.debug("debug msg3 {} {}", 1, "t");
        logger.debug("debug msg2", new SQLException("test"));

        assertEquals(logDebug, logger.isTraceEnabled());
        logger.trace("trace msg");
        logger.trace("trace msg3 {} {}", 1, "t");
        logger.trace("trace msg2", new SQLException("test"));

        String errSt = new String(err.toByteArray());
        String outSt = new String(out.toByteArray());

        assertTrue(
            errSt.contains(
                "[ERROR] (main) error msg\n"
                    + "[ERROR] (main) error msg3 1 t\n"
                    + "[ERROR] (main) error msg4 null\n"
                    + "[ERROR] (main) null\n"
                    + "[ERROR] (main) error msg2 - java.sql.SQLException: test"));
        assertTrue(
            errSt.contains(
                "[ WARN] (main) warn msg\n"
                    + "[ WARN] (main) warn msg3 1 t\n"
                    + "[ WARN] (main) warn msg2 - java.sql.SQLException: test"));
        assertTrue(
            outSt.contains(
                "[ INFO] (main) info msg\n"
                    + "[ INFO] (main) info msg3 1 t\n"
                    + "[ INFO] (main) info msg2 - java.sql.SQLException: test"));
        if (logDebug) {
          assertTrue(
              outSt.contains(
                  "[DEBUG] (main) debug msg\n"
                      + "[DEBUG] (main) debug msg3 1 t\n"
                      + "[DEBUG] (main) debug msg2 - java.sql.SQLException: test"));
          assertTrue(
              outSt.contains(
                  "[TRACE] (main) trace msg\n"
                      + "[TRACE] (main) trace msg3 1 t\n"
                      + "[TRACE] (main) trace msg2 - java.sql.SQLException: test"));
        } else {
          assertFalse(outSt.contains("[DEBUG]"));
          assertFalse(outSt.contains("[TRACE]"));
        }
      }
    }
  }
}
