// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.sql.PooledConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.mariadb.jdbc.Statement;
import org.slf4j.LoggerFactory;

public class LoggingTest extends Common {

  @Test
  void basicLogging() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer());
    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
    Level initialLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    logger.setAdditive(false);
    logger.detachAndStopAllAppenders();

    LoggerContext context = new LoggerContext();
    FileAppender<ILoggingEvent> fa = new FileAppender<>();
    fa.setName("FILE");
    fa.setImmediateFlush(true);
    PatternLayoutEncoder pa = new PatternLayoutEncoder();
    pa.setPattern("%r %5p %c [%t] - %m%n");
    pa.setContext(context);
    pa.start();
    fa.setEncoder(pa);

    fa.setFile(tempFile.getPath());
    fa.setAppend(true);
    fa.setContext(context);
    fa.start();

    logger.addAppender(fa);

    try (Connection conn = createCon()) {
      Statement stmt = conn.createStatement();
      stmt.execute("SELECT 1");
    }
    try (Connection conn = createCon("useCompression=true")) {
      Statement stmt = conn.createStatement();
      stmt.execute("SELECT 1");
    }

    MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(
            mDefUrl + "&sessionVariables=wait_timeout=1&maxIdleTime=2&testMinRemovalDelay=2");
    Thread.sleep(4000);
    PooledConnection pc = ds.getPooledConnection();
    pc.getConnection().isValid(1);
    pc.close();
    ds.close();
    try {
      String contents = new String(Files.readAllBytes(Path.of(tempFile.getPath())));
      String selectOne =
          "       +--------------------------------------------------+\n"
              + "       |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
              + "+------+--------------------------------------------------+------------------+\n"
              + "|000000| 09 00 00 00 03 53 45 4C  45 43 54 20 31          | .....SELECT 1    |\n"
              + "+------+--------------------------------------------------+------------------+\n";
      Assertions.assertTrue(
          contents.contains(selectOne) || contents.contains(selectOne.replace("\r\n", "\n")),
          contents);
      String rowResult =
          "       +--------------------------------------------------+\n"
              + "       |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
              + "+------+--------------------------------------------------+------------------+\n"
              + "|000000| 02 00 00 03 01 31                                | .....1           |\n"
              + "+------+--------------------------------------------------+------------------+\n";
      String rowResultWithEof =
          "       +--------------------------------------------------+\n"
              + "       |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
              + "+------+--------------------------------------------------+------------------+\n"
              + "|000000| 02 00 00 04 01 31                                | .....1           |\n"
              + "+------+--------------------------------------------------+------------------+\n";
      Assertions.assertTrue(
          contents.contains(rowResult)
              || contents.contains(rowResult.replace("\r\n", "\n"))
              || contents.contains(rowResultWithEof)
              || contents.contains(rowResultWithEof.replace("\r\n", "\n")),
          contents);

      Assertions.assertTrue(
          contents.contains("pool MariaDB-pool new physical connection ")
              && contents.contains("created (total:1, active:0, pending:0)"),
          contents);
      Assertions.assertTrue(
          contents.contains("pool MariaDB-pool connection ")
              && contents.contains("removed due to inactivity"),
          contents);
    } catch (IOException e) {
      e.printStackTrace();
      Assertions.fail();
    } finally {
      logger.setLevel(initialLevel);
      logger.detachAppender(fa);
    }
  }
}
