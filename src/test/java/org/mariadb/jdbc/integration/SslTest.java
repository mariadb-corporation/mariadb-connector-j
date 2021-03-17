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

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

@DisplayName("SSL tests")
public class SslTest extends Common {
  private static Integer sslPort;
  private static String baseOptions = "&user=serverAuthUser&password=!Passw0rd3Works";
  private static String baseMutualOptions = "&user=mutualAuthUser&password=!Passw0rd3Works";

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS serverAuthUser");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Assumptions.assumeTrue(haveSsl());
    createSslUser("serverAuthUser", "REQUIRE SSL");
    createSslUser("mutualAuthUser", "REQUIRE X509");

    Statement stmt = sharedConn.createStatement();
    stmt.execute("FLUSH PRIVILEGES");
    sslPort =
        System.getenv("TEST_MAXSCALE_TLS_PORT") == null || System.getenv("TEST_MAXSCALE_TLS_PORT").isEmpty()
            ? null
            : Integer.valueOf(System.getenv("TEST_MAXSCALE_TLS_PORT"));
  }

  private static void createSslUser(String user, String requirement) throws SQLException {
    boolean useOldNotation = true;
    if ((isMariaDBServer() && minVersion(10, 2, 0))
        || (!isMariaDBServer() && minVersion(8, 0, 0))) {
      useOldNotation = false;
    }
    Statement stmt = sharedConn.createStatement();
    if (useOldNotation) {
      stmt.execute("CREATE USER IF NOT EXISTS '" + user + "'@'%' " + requirement);
      stmt.execute(
          "GRANT SELECT ON *.* TO '"
              + user
              + "'@'%' IDENTIFIED BY '!Passw0rd3Works' "
              + requirement);
    } else {
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        stmt.execute(
            "CREATE USER IF NOT EXISTS '"
                + user
                + "'@'%' IDENTIFIED WITH mysql_native_password BY '!Passw0rd3Works' "
                + requirement);
      } else {
        stmt.execute(
            "CREATE USER IF NOT EXISTS '"
                + user
                + "'@'%' IDENTIFIED BY '!Passw0rd3Works' "
                + requirement);
      }
      stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO '" + user + "'@'%' ");
    }
  }

  private String getSslVersion(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("show STATUS  LIKE 'Ssl_version'");
    if (rs.next()) {
      return rs.getString(2);
    }
    return null;
  }

  @Test
  public void simpleSsl() throws SQLException {
    try (Connection con = createCon("sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
  }

  @Test
  public void mandatorySsl() throws SQLException {
    if (!"maxscale".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))) {
      try (Connection con = createCon(baseOptions + "&sslMode=trust", sslPort)) {
        assertNotNull(getSslVersion(con));
      }
      assertThrows(SQLException.class, () -> createCon(baseOptions + "&sslMode=disable"));
      assertThrows(
          SQLInvalidAuthorizationSpecException.class,
          () -> createCon(baseMutualOptions + "&sslMode=trust", sslPort));
    }
  }

  @Test
  public void certificateMandatorySsl() throws Throwable {

    String serverCertPath = retrieveCertificatePath();
    Assumptions.assumeTrue(serverCertPath != null, "Canceled, server certificate not provided");

    // certificate path, like  /path/certificate.crt
    try (Connection con =
        createCon(
            baseOptions + "&sslMode=VERIFY_CA" + "&serverSslCert=" + serverCertPath, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    String urlPath = Paths.get(serverCertPath).toUri().toURL().toString();
    // file certificate path, like  file:/path/certificate.crt
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA" + "&serverSslCert=" + urlPath, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    String certificateString = getServerCertificate(serverCertPath);
    // file certificate, like  -----BEGIN CERTIFICATE-----...
    try (Connection con =
        createCon(
            baseOptions + "&sslMode=VERIFY_CA" + "&serverSslCert=" + certificateString, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    assertThrows(
        SQLNonTransientConnectionException.class,
        () -> createCon(baseOptions + "&sslMode=VERIFY_CA", sslPort));
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            createCon(
                baseMutualOptions + "&sslMode=VERIFY_CA" + "&serverSslCert=" + serverCertPath,
                sslPort));
  }

  private String getServerCertificate(String serverCertPath) throws SQLException {
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(serverCertPath)))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
      return sb.toString();
    } catch (Exception e) {
      throw new SQLException("abnormal exception", e);
    }
  }

  private String retrieveCertificatePath() throws Exception {
    String serverCertificatePath = checkFileExists(System.getProperty("serverCertificatePath"));

    // try local server
    if (serverCertificatePath == null
        && !"skysql".equals(System.getenv("srv"))
        && !"skysql-ha".equals(System.getenv("srv"))) {

      try (ResultSet rs = sharedConn.createStatement().executeQuery("select @@ssl_cert")) {
        assertTrue(rs.next());
        serverCertificatePath = checkFileExists(rs.getString(1));
      }
    }
    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists("../../ssl/server.crt");
    }
    return serverCertificatePath;
  }

  private String checkFileExists(String path) throws IOException {
    if (path == null) return null;
    File f = new File(path);
    if (f.exists()) {
      return f.getCanonicalPath().replace("\\", "/");
    }
    return null;
  }
}
