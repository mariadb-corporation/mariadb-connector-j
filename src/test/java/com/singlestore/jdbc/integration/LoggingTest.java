// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.SingleStorePoolDataSource;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.tls.HostnameVerifier;
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
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class LoggingTest extends Common {

  @Test
  void basicLogging() throws Exception {
    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("com.singlestore.jdbc");
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

    SingleStorePoolDataSource ds =
        new SingleStorePoolDataSource(
            mDefUrl + "&sessionVariables=wait_timeout=1&maxIdleTime=2&testMaxRemovalDelay=2");
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
          contents.contains("pool SingleStore-pool new physical connection ")
              && contents.contains("created (total:1, active:0, pending:0)"),
          contents);
      Assertions.assertTrue(
          contents.contains("pool SingleStore-pool connection ")
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

    Logger logger = (Logger) LoggerFactory.getLogger("com.singlestore.jdbc");
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

    String certString =
        ""
            + "-----BEGIN CERTIFICATE-----\n"
            + "MIIDgjCCAmqgAwIBAgIUa5fj2ylHRanZ8B1vxNLDoUn3YfwwDQYJKoZIhvcNAQEL\n"
            + "BQAwUzELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYDVQQHDAJTWjETMBEG\n"
            + "A1UECgwKQWNtZSwgSW5jLjEVMBMGA1UEAwwMQWNtZSBSb290IENBMCAXDTIxMTAw\n"
            + "NzE0MTY1NVoYDzIxMjEwOTEzMTQxNjU1WjBYMQswCQYDVQQGEwJDTjELMAkGA1UE\n"
            + "CAwCR0QxCzAJBgNVBAcMAlNaMRMwEQYDVQQKDApBY21lLCBJbmMuMRowGAYDVQQD\n"
            + "DBEqLnNpbmdsZXN0b3JlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
            + "ggEBAN/WPxXQzFFSIdyeBqn76rZoHRvW+iAEK/y4lxnIWP3wus71GzcO4rFuISQO\n"
            + "ofRTIiIFzzCcNCyF478rddiuNeTlqsJgeC9pNPTrOr77T2qgTzyGTfImtnj1SU4Z\n"
            + "ef1QWoIOaQm52cM3T2H+UPkVrnOy7G5gbYyzCmOwx2SCD3XoVz/oDqVW6QCFNIIw\n"
            + "nq6dNuvLU65mGjInQVpeB4x0uWkm5Q5xHESTu8gsxKJOeuW5ehq9AN3ohid/Pr6R\n"
            + "ohRbtQlGv9xKOzrYZczYt27RmT+MrGLrWMGCrMR72zzm6bST3OXMPIDZKDkb32Jx\n"
            + "5r/9xqfhq2N+AwlKbHpqeBWM4YkCAwEAAaNHMEUwQwYDVR0RBDwwOoIVbG9jYWxo\n"
            + "b3N0LmxvY2FsZG9tYWlugglsb2NhbGhvc3SHBH8AAAGHECABDbg5AjRoAAAAAAAA\n"
            + "BEMwDQYJKoZIhvcNAQELBQADggEBAE9Jo5LGT/c8dVAKIsInk+9SsSkaR7bPLdKU\n"
            + "pbBttV9flKDvD6JvAmfw+snUp7Zcrm0IVnxFCP3uKCcLXCyFA0eu6w7CrNAfI8a2\n"
            + "Aw9zKDTq0MaawCmQ0wjCOoHTQn/3tewGTAToppDA++q7HgpEeC+6zyz3iP/mPk0d\n"
            + "rfKVTFH9hVtimUnR16uN/P1zVemnQ/1zqsRhfQ1ua9fjKaLRweUvygdiz2uhsHDY\n"
            + "EQFky1SW+gJhLJCqVxx6k4SdbymZNHn/SV0Fj/gt1H4UndM12S9JxSq8fbdL2mHi\n"
            + "CWSKz9FrVwpCcX0v7XpSIzJhvM9isgF6IA90UkExmmpOwuyKuH0=\n"
            + "-----END CERTIFICATE-----\n";
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    X509Certificate cert =
        (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(certString.getBytes()));

    assertEquals(
        new X500Principal("CN=*.singlestore.com, O=\"Acme, Inc.\", L=SZ, ST=GD, C=CN"),
        cert.getSubjectX500Principal());
    HostnameVerifier.verify("localhost", cert, -1);
    HostnameVerifier.verify("localhost.localdomain", cert, -1);
    verifyExceptionEqual(
        "local.host",
        cert,
        "DNS host \"local.host\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    HostnameVerifier.verify("127.0.0.1", cert, -1);
    verifyExceptionEqual(
        "127.0.0.2",
        cert,
        "IPv4 host \"127.0.0.2\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    HostnameVerifier.verify("2001:db8:3902:3468:0:0:0:443", cert, -1);
    verifyExceptionEqual(
        "2001:db8:1::",
        cert,
        "IPv6 host \"2001:db8:1::\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");
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
