// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.unit.util.log;

import static org.junit.jupiter.api.Assertions.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.io.*;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.tidb.jdbc.util.log.Logger;
import org.tidb.jdbc.util.log.Loggers;

public class LoggersTest {

  @Test
  public void Slf4JLogger() throws IOException {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(this.getClass());
    root.setLevel(Level.TRACE);

    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    PatternLayoutEncoder ple = new PatternLayoutEncoder();
    ple.setPattern("%level %logger{10} [%file] %msg%n");
    ple.setContext(lc);
    ple.start();

    File tempfile = File.createTempFile("debug-", ".log");
    FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
    fileAppender.setFile(tempfile.getCanonicalPath());
    fileAppender.setEncoder(ple);
    fileAppender.setContext(lc);
    fileAppender.start();

    root.addAppender(fileAppender);
    Logger logger = Loggers.getLogger(LoggersTest.class);
    assertTrue(logger.isErrorEnabled());
    assertTrue(logger.isWarnEnabled());
    assertTrue(logger.isInfoEnabled());
    assertTrue(logger.isDebugEnabled());
    assertTrue(logger.isTraceEnabled());
    assertEquals("org.tidb.jdbc.unit.util.log.LoggersTest", logger.getName());
    Logger logger2 = Loggers.getLogger("bla");
    assertEquals("bla", logger2.getName());

    logger.error("Test err1");
    logger.error("Test err2", new SQLException("test exception"));
    logger.error("Test err3 {}", "param");

    logger.warn("Test warn 1");
    logger.warn("Test warn 2", new SQLException("test exception"));
    logger.warn("Test warn 3 {}", "param");

    logger.info("Test info 1");
    logger.info("Test info 2", new SQLException("test exception"));
    logger.info("Test info 3 {}", "param");

    logger.debug("Test debug 1");
    logger.debug("Test debug 2", new SQLException("test exception"));
    logger.debug("Test debug 3 {}", "param");

    logger.trace("Test trace 1");
    logger.trace("Test trace 2", new SQLException("test exception"));
    logger.trace("Test trace 3 {}", "param");

    BufferedReader reader = new BufferedReader(new FileReader(tempfile.getCanonicalPath()));
    assertEquals("ERROR o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test err1", reader.readLine());
    assertEquals("ERROR o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test err2", reader.readLine());
    assertEquals("java.sql.SQLException: test exception", reader.readLine());
    String line;
    do {
      line = reader.readLine();
    } while (line != null && line.startsWith("\tat "));

    assertEquals("ERROR o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test err3 param", line);
    assertEquals("WARN o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test warn 1", reader.readLine());
    assertEquals("WARN o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test warn 2", reader.readLine());
    assertEquals("java.sql.SQLException: test exception", reader.readLine());

    do {
      line = reader.readLine();
    } while (line != null && line.startsWith("\tat "));

    assertEquals("WARN o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test warn 3 param", line);
    assertEquals("INFO o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test info 1", reader.readLine());
    assertEquals("INFO o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test info 2", reader.readLine());
    assertEquals("java.sql.SQLException: test exception", reader.readLine());

    do {
      line = reader.readLine();
    } while (line != null && line.startsWith("\tat "));

    assertEquals("INFO o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test info 3 param", line);
    assertEquals(
        "DEBUG o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test debug 1", reader.readLine());
    assertEquals(
        "DEBUG o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test debug 2", reader.readLine());
    assertEquals("java.sql.SQLException: test exception", reader.readLine());

    do {
      line = reader.readLine();
    } while (line != null && line.startsWith("\tat "));

    assertEquals("DEBUG o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test debug 3 param", line);
    assertEquals(
        "TRACE o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test trace 1", reader.readLine());
    assertEquals(
        "TRACE o.t.j.u.u.l.LoggersTest [Slf4JLogger.java] Test trace 2", reader.readLine());
    assertEquals("java.sql.SQLException: test exception", reader.readLine());

    reader.close();
  }
}
