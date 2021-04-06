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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLException;
import javax.security.auth.x500.X500Principal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.tls.HostnameVerifierImpl;
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
      System.out.println(contents);
      String defaultRequest =
          "+--------------------------------------------------+\n"
              + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
              + "+--------------------------------------------------+------------------+\n"
              + "| CD 00 00 00 03 73 65 74  20 61 75 74 6F 63 6F 6D | .....set autocom |\n"
              + "| 6D 69 74 3D 31 2C 20 73  71 6C 5F 6D 6F 64 65 20 | mit=1, sql_mode  |\n"
              + "| 3D 20 63 6F 6E 63 61 74  28 40 40 73 71 6C 5F 6D | = concat(@@sql_m |\n"
              + "| 6F 64 65 2C 27 2C 53 54  52 49 43 54 5F 54 52 41 | ode,',STRICT_TRA |\n"
              + "| 4E 53 5F 54 41 42 4C 45  53 27 29 2C 20 73 65 73 | NS_TABLES'), ses |\n"
              + "| 73 69 6F 6E 5F 74 72 61  63 6B 5F 73 63 68 65 6D | sion_track_schem |\n"
              + "| 61 3D 31 2C 74 78 5F 69  73 6F 6C 61 74 69 6F 6E | a=1,tx_isolation |\n"
              + "| 3D 27 52 45 50 45 41 54  41 42 4C 45 2D 52 45 41 | ='REPEATABLE-REA |\n"
              + "| 44 27 3B 53 45 4C 45 43  54 20 40 40 6D 61 78 5F | D';SELECT @@max_ |\n"
              + "| 61 6C 6C 6F 77 65 64 5F  70 61 63 6B 65 74 2C 20 | allowed_packet,  |\n"
              + "| 40 40 77 61 69 74 5F 74  69 6D 65 6F 75 74 3B 53 | @@wait_timeout;S |\n"
              + "| 45 54 20 53 45 53 53 49  4F 4E 20 54 52 41 4E 53 | ET SESSION TRANS |\n"
              + "| 41 43 54 49 4F 4E 20 52  45 41 44 20 57 52 49 54 | ACTION READ WRIT |\n"
              + "| 45                                               | E                |\n"
              + "+--------------------------------------------------+------------------+";
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
              + "| 02 00 00 04 01 31                                | .....1           |\n"
              + "+--------------------------------------------------+------------------+\n";
      if ("maxscale".equals(System.getenv("srv")) || "skysql-ha".equals(System.getenv("srv"))) {
        Assertions.assertTrue(
            contents.contains(rowResultWithEof)
                || contents.contains(rowResultWithEof.replace("\r\n", "\n")));
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

  HostnameVerifierImpl verifier = new HostnameVerifierImpl();

  @Test
  void certLogging() throws Exception {
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
    HostnameVerifierImpl verifier = new HostnameVerifierImpl();

    fa.setAppend(true);
    fa.setContext(context);
    fa.start();

    logger.addAppender(fa);

    String certString =
        ""
            + "-----BEGIN CERTIFICATE-----\n"
            + "MIIDfDCCAmSgAwIBAgIURZJQVOWv+oaj+MLlHWc1B0TnOaowDQYJKoZIhvcNAQEL\n"
            + "BQAwUjELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYDVQQHDAJTWjESMBAG\n"
            + "A1UECgwJQWNtZSxJbmMuMRUwEwYDVQQDDAxBY21lIFJvb3QgQ0EwIBcNMjEwMzMw\n"
            + "MDkwODAxWhgPMjEyMTAzMDYwOTA4MDFaMFMxCzAJBgNVBAYTAkNOMQswCQYDVQQI\n"
            + "DAJHRDELMAkGA1UEBwwCU1oxEjAQBgNVBAoMCUFjbWUsSW5jLjEWMBQGA1UEAwwN\n"
            + "Ki5tYXJpYWRiLm9yZzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANAJ\n"
            + "xqbqTGmwO5n3kVd6QJPRSh+0M1HIQacyM/tkE7jLw3725/KtknuwuFbPpxKyTCLC\n"
            + "IoNx4yaBbmx783OPP3pokXTWiMdrVZdLltBNamNzekNFN4YhR5oN479M5cKgrk94\n"
            + "Ud+ql0NN5FscrSQ0fSdJf0idJMqThro1MJVp9rp5cdCba6/lKyDbdOybe5f7rmrg\n"
            + "+37J+src67+rqwVT8ZwZgLTGDf4X9OSIzyw6+PCWYWr89aurrOuOyqA3QqXVRZa/\n"
            + "IxOMHIdzXMgLN6+HduwdZ+DNv1NPT2MDlRQvOnDop3NoEVKWekOTv50LbKRgWTYO\n"
            + "TK/dfcsDpZmdyHv7pb8CAwEAAaNHMEUwQwYDVR0RBDwwOoIVbG9jYWxob3N0Lmxv\n"
            + "Y2FsZG9tYWlugglsb2NhbGhvc3SHBH8AAAGHECABDbg5AjRoAAAAAAAABEMwDQYJ\n"
            + "KoZIhvcNAQELBQADggEBAHsiJz9cpmL8BTa/o10S+pmap3iOnYYuJT0llCRLJ+Ji\n"
            + "msO2niyIwqCJHMLcEABCENJt0HDOEKlnunVgc+X/6K8DnPrYhfWQbYI/dwUBoSIQ\n"
            + "siK/yKW0q+S+YjCVpNMA3iMfhJ9Qe9LDO+xdCBhzplgrV8YwG+J2FUNbZfvl5cML\n"
            + "TjKLWrWo9dgZyH/7mjwryRzswfUfr/lRARCyrMotaXfYmjPjwTSRc0aPGrEjs3ns\n"
            + "WMtimgh7Zw3Tbxc51miz9CRy767lq/9BGTdeBLmW0EXssIJb9uO0Ht3C/Pqy0ojk\n"
            + "8e1eYtofjTsqWHZ1s2LhtT0HvXdL6BnWP9GWc/zxiKM=\n"
            + "-----END CERTIFICATE-----\n";
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    X509Certificate cert =
        (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(certString.getBytes()));

    assertEquals(
        new X500Principal("CN=*.mariadb.org, O=\"Acme,Inc.\", L=SZ, ST=GD, C=CN"),
        cert.getSubjectX500Principal());
    verifier.verify("localhost", cert, -1);
    verifier.verify("localhost.localdomain", cert, -1);
    verifyExceptionEqual(
        "local.host",
        cert,
        "DNS host \"local.host\" doesn't correspond to certificate "
            + "CN \"*.mariadb.org\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    verifier.verify("127.0.0.1", cert, -1);
    verifyExceptionEqual(
        "127.0.0.2",
        cert,
        "IPv4 host \"127.0.0.2\" doesn't correspond to certificate "
            + "CN \"*.mariadb.org\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    verifier.verify("2001:db8:3902:3468:0:0:0:443", cert, -1);
    verifyExceptionEqual(
        "2001:db8:1::",
        cert,
        "IPv6 host \"2001:db8:1::\" doesn't correspond to certificate "
            + "CN \"*.mariadb.org\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");
    try {
      String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));

      assertTrue(
          contents.contains(
              "DNS verification of hostname : type=DNS value=localhost.localdomain to local.host"));
      assertTrue(
          contents.contains(
              "IPv4 verification of hostname : type=IP value=127.0.0.1 to 127.0.0.2"));
      assertTrue(
          contents.contains(
              "IPv6 verification of hostname : type=IP value=2001:db8:3902:3468:0:0:0:443 to 2001:db8:1::"));

      logger.setLevel(initialLevel);
      logger.detachAppender(fa);
    } catch (IOException e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }

  private void verifyExceptionEqual(String host, X509Certificate cert, String exceptionMessage) {
    Exception e =
        Assertions.assertThrows(SSLException.class, () -> verifier.verify(host, cert, -1));
    Assertions.assertTrue(
        e.getMessage().contains(exceptionMessage), "real message:" + e.getMessage());
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
