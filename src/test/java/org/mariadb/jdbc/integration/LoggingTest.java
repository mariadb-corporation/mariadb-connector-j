// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

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
import javax.sql.PooledConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.tls.HostnameVerifier;
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
      String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));
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

  @Test
  void certLogging() throws Exception {
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
    HostnameVerifier.verify("localhost", cert, -1);
    HostnameVerifier.verify("localhost.localdomain", cert, -1);
    verifyExceptionEqual(
        "local.host",
        cert,
        "DNS host \"local.host\" doesn't correspond to certificate "
            + "CN \"*.mariadb.org\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    HostnameVerifier.verify("127.0.0.1", cert, -1);
    verifyExceptionEqual(
        "127.0.0.2",
        cert,
        "IPv4 host \"127.0.0.2\" doesn't correspond to certificate "
            + "CN \"*.mariadb.org\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    HostnameVerifier.verify("2001:db8:3902:3468:0:0:0:443", cert, -1);
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
        Assertions.assertThrows(SSLException.class, () -> HostnameVerifier.verify(host, cert, -1));
    Assertions.assertTrue(
        e.getMessage().contains(exceptionMessage), "real message:" + e.getMessage());
  }
}
