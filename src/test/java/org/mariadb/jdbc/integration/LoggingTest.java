/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

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
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.slf4j.LoggerFactory;

public class LoggingTest extends Common {

  @Test
  void basicLogging() throws Exception {
    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
    Level initialLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    logger.setAdditive(false);
    logger.detachAndStopAllAppenders();

    LoggerContext context = new LoggerContext();
    FileAppender<ILoggingEvent> fa = new FileAppender<ILoggingEvent>();
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
    try {
      String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));
      String defaultRequest =
          "+--------------------------------------------------+\n"
              + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
              + "+--------------------------------------------------+------------------+\n"
              + "| 5F 00 00 00 03 73 65 74  20 61 75 74 6F 63 6F 6D | _....set autocom |\n"
              + "| 6D 69 74 3D 31 2C 20 73  71 6C 5F 6D 6F 64 65 20 | mit=1, sql_mode  |\n"
              + "| 3D 20 63 6F 6E 63 61 74  28 40 40 73 71 6C 5F 6D | = concat(@@sql_m |\n"
              + "| 6F 64 65 2C 27 2C 53 54  52 49 43 54 5F 54 52 41 | ode,',STRICT_TRA |\n"
              + "| 4E 53 5F 54 41 42 4C 45  53 27 29 2C 20 73 65 73 | NS_TABLES'), ses |\n"
              + "| 73 69 6F 6E 5F 74 72 61  63 6B 5F 73 63 68 65 6D | sion_track_schem |\n"
              + "| 61 3D 31                                         | a=1              |\n"
              + "+--------------------------------------------------+------------------+\n";
      if (!"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv"))) {
        Assertions.assertTrue(
            contents.contains(defaultRequest)
                || contents.contains(defaultRequest.replace("\r\n", "\n")));
      }
      String selectOne =
          "+--------------------------------------------------+\n"
              + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
              + "+--------------------------------------------------+------------------+\n"
              + "| 09 00 00 00 03 53 45 4C  45 43 54 20 31          | .....SELECT 1    |\n"
              + "+--------------------------------------------------+------------------+\n";
      Assertions.assertTrue(
          contents.contains(selectOne) || contents.contains(selectOne.replace("\r\n", "\n")));
      String rowResult =
          "+--------------------------------------------------+\n"
              + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
              + "+--------------------------------------------------+------------------+\n"
              + "| 02 00 00 03 01 31                                | .....1           |\n"
              + "+--------------------------------------------------+------------------+\n";
      String rowResultWithEof =
              "+--------------------------------------------------+\n"
                      + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
                      + "+--------------------------------------------------+------------------+\n"
                      + "| 03 00 00 03 01 31                                | .....1           |\n"
                      + "+--------------------------------------------------+------------------+\n";
      if ("maxscale".equals(System.getenv("srv")) || "skysql-ha".equals(System.getenv("srv"))) {
        System.out.println(contents);
        Assertions.assertTrue(
                contents.contains(rowResultWithEof) || contents.contains(rowResultWithEof.replace("\r\n", "\n")));
      } else {
        Assertions.assertTrue(
                contents.contains(rowResult) || contents.contains(rowResult.replace("\r\n", "\n")));
      }
      logger.setLevel(initialLevel);
      logger.detachAppender(fa);
    } catch (IOException e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }

  public String encodeHexString(byte[] byteArray) {
    StringBuffer hexStringBuffer = new StringBuffer();
    for (int i = 0; i < byteArray.length; i++) {
      hexStringBuffer.append(byteToHex(byteArray[i]));
    }
    return hexStringBuffer.toString();
  }

  public String byteToHex(byte num) {
    char[] hexDigits = new char[2];
    hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
    hexDigits[1] = Character.forDigit((num & 0xF), 16);
    return new String(hexDigits);
  }
}
