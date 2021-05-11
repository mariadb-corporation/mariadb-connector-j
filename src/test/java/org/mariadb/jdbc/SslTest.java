/*
 *
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import org.junit.*;
import org.mariadb.jdbc.failover.TcpProxy;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class SslTest extends BaseTest {

  private static Integer sslPort;
  private static String baseOptions = "&user=serverAuthUser&password=!Passw0rd3Works";
  private static String baseMutualOptions = "&user=mutualAuthUser&password=!Passw0rd3Works";

  @AfterClass
  public static void drop() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("DROP USER IF EXISTS serverAuthUser");
    stmt.execute("DROP USER IF EXISTS mutualAuthUser");
  }

  @BeforeClass
  public static void beforeAll2() throws SQLException {
    drop();
    Assume.assumeTrue(haveSsl(sharedConnection));
    createSslUser("serverAuthUser", "REQUIRE SSL");
    createSslUser("mutualAuthUser", "REQUIRE X509");

    Statement stmt = sharedConnection.createStatement();
    stmt.execute("FLUSH PRIVILEGES");
    sslPort =
        System.getenv("TEST_MAXSCALE_TLS_PORT") == null
                || System.getenv("TEST_MAXSCALE_TLS_PORT").isEmpty()
            ? port
            : Integer.valueOf(System.getenv("TEST_MAXSCALE_TLS_PORT"));
  }

  private static void createSslUser(String user, String requirement) throws SQLException {
    boolean useOldNotation = true;
    if ((isMariadbServer() && minVersion(10, 2, 0))
        || (!isMariadbServer() && minVersion(8, 0, 0))) {
      useOldNotation = false;
    }
    Statement stmt = sharedConnection.createStatement();
    if (useOldNotation) {
      stmt.execute("CREATE USER IF NOT EXISTS '" + user + "'@'%' " + requirement);
      stmt.execute(
          "GRANT SELECT ON *.* TO '"
              + user
              + "'@'%' IDENTIFIED BY '!Passw0rd3Works' "
              + requirement);
    } else {
      if (!isMariadbServer() && minVersion(8, 0, 0)) {
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
      stmt.execute(
          "GRANT SELECT ON " + sharedConnection.getCatalog() + ".* TO '" + user + "'@'%' ");
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
    try (Connection con = setConnection("&useSsl&trustServerCertificate=true", database, sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con =
        setConnection(
            "&useSsl&trustServerCertificate=true&useReadAheadInput=false", database, sslPort)) {
      assertNotNull(getSslVersion(con));
    }
  }

  @Test
  public void mandatorySsl() throws SQLException {
    Assume.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con =
        setConnection(baseOptions + "&useSsl&trustServerCertificate=true", database, sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrows(SQLException.class, () -> setConnection(baseOptions + "&useSsl=false"));
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            setConnection(
                baseMutualOptions + "&useSsl&trustServerCertificate=true", database, sslPort));
  }

  @Test
  public void enabledSslProtocolSuites() throws SQLException {
    Assume.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con =
        setConnection(
            baseOptions
                + "&useSsl&trustServerCertificate=true&enabledSslProtocolSuites=TLSv1.2,TLSv1.3",
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try {
      setConnection(
          baseMutualOptions + "&useSsl&trustServerCertificate=true&enabledSslProtocolSuites=SSLv3",
          database,
          sslPort);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("No appropriate protocol"));
    }
    try {
      setConnection(
          baseMutualOptions
              + "&useSsl&trustServerCertificate=true&enabledSslProtocolSuites=unknown",
          database,
          sslPort);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Unsupported SSL protocol 'unknown'"));
    }
  }

  @Test
  public void enabledSslCipherSuites() throws SQLException {
    Assume.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con =
        setConnection(
            baseOptions
                + "&useSsl&trustServerCertificate=true&enabledSslCipherSuites=TLS_DHE_RSA_WITH_AES_256_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try {
      setConnection(
          baseMutualOptions
              + "&useSsl&trustServerCertificate=true&enabledSslCipherSuites=UNKNOWN_CIPHER",
          database,
          sslPort);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Unsupported SSL cipher"));
    }
  }

  @Test
  public void mutualAuthSsl() throws SQLException {
    Assume.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assume.assumeTrue(System.getenv("TEST_DB_CLIENT_PKCS") != null);

    // without password
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            setConnection(
                baseMutualOptions
                    + "&useSsl&trustServerCertificate=true&keyStore="
                    + System.getenv("TEST_DB_CLIENT_PKCS"),
                database,
                sslPort));
    // with password
    try (Connection con =
        setConnection(
            baseMutualOptions
                + "&useSsl&trustServerCertificate=true&keyStore="
                + System.getenv("TEST_DB_CLIENT_PKCS")
                + "&keyStorePassword=kspass",
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // with URL
    boolean isWin = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
    try (Connection con =
        setConnection(
            baseMutualOptions
                + "&useSsl&trustServerCertificate=true&keyStore="
                + "file://"
                + (isWin ? "/" : "")
                + System.getenv("TEST_DB_CLIENT_PKCS")
                + "&keyStorePassword=kspass",
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // wrong keystore type
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            setConnection(
                baseMutualOptions
                    + "&useSsl&trustServerCertificate=true&keyStoreType=JKS&keyStore="
                    + System.getenv("TEST_DB_CLIENT_PKCS"),
                database,
                sslPort));
    // good keystore type
    try (Connection con =
        setConnection(
            baseMutualOptions
                + "&useSsl&trustServerCertificate=true&keyStoreType=pkcs12&keyStore="
                + System.getenv("TEST_DB_CLIENT_PKCS")
                + "&keyStorePassword=kspass",
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // with system properties
    System.setProperty("javax.net.ssl.keyStore", System.getenv("TEST_DB_CLIENT_PKCS"));
    System.setProperty("javax.net.ssl.keyStorePassword", "kspass");
    try (Connection con =
        setConnection(
            baseMutualOptions + "&useSsl&trustServerCertificate=true", database, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // wrong keystore type
    System.setProperty("javax.net.ssl.keyStoreType", "JKS");
    try (Connection con =
        setConnection(
            baseMutualOptions + "&useSsl&trustServerCertificate=true", database, sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con =
        setConnection(
            baseMutualOptions + "&useSsl&trustServerCertificate=true", database, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    System.clearProperty("javax.net.ssl.keyStoreType");
    try (Connection con =
        setConnection(
            baseMutualOptions + "&useSsl&trustServerCertificate=true&keyStoreType=JKS",
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // without password
    System.clearProperty("javax.net.ssl.keyStorePassword");
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            setConnection(
                baseMutualOptions + "&useSsl&trustServerCertificate=true", database, sslPort));
  }

  @Test
  public void certificateMandatorySsl() throws Throwable {

    String serverCertPath = retrieveCertificatePath();
    Assume.assumeTrue("Canceled, server certificate not provided", serverCertPath != null);

    // certificate path, like  /path/certificate.crt
    try (Connection con =
        setConnection(
            baseOptions + "&useSsl&disableSslHostnameVerification&serverSslCert=" + serverCertPath,
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    if (!"localhost".equals(hostname)) {
      try (Connection con =
          setConnection(
              baseOptions + "&useSsl&serverSslCert=" + serverCertPath, database, sslPort)) {
        assertNotNull(getSslVersion(con));
      }

      UrlParser conf = UrlParser.parse(mDefUrl);
      HostAddress hostAddress = conf.getHostAddresses().get(0);
      try {
        proxy = new TcpProxy(hostAddress.host, sslPort == null ? hostAddress.port : sslPort);
      } catch (IOException i) {
        throw new SQLException("proxy error", i);
      }

      String url = mDefUrl.replaceAll("//([^/]*)/", "//localhost:" + proxy.getLocalPort() + "/");
      try {
        DriverManager.getConnection(url + "&useSsl&serverSslCert=" + serverCertPath);
        fail();
      } catch (SQLException e) {
        assertTrue(
            e.getMessage().contains("DNS host \"localhost\" doesn't correspond to certificate"));
      }
    }

    String urlPath = Paths.get(serverCertPath).toUri().toURL().toString();
    // file certificate path, like  file:/path/certificate.crt
    try (Connection con =
        setConnection(
            baseOptions + "&useSsl&disableSslHostnameVerification&serverSslCert=" + urlPath,
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    String certificateString = getServerCertificate(serverCertPath);
    // file certificate, like  -----BEGIN CERTIFICATE-----...
    try (Connection con =
        setConnection(
            baseOptions
                + "&useSsl&disableSslHostnameVerification&serverSslCert="
                + certificateString,
            database,
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    assertThrows(
        SQLNonTransientConnectionException.class,
        () ->
            setConnection(
                baseOptions + "&useSsl&disableSslHostnameVerification", database, sslPort));
    if (!"skysql-ha".equals(System.getenv("srv")) && !"maxscale".equals(System.getenv("srv"))) {
      assertThrows(
          SQLInvalidAuthorizationSpecException.class,
          () ->
              setConnection(
                  baseMutualOptions
                      + "&useSsl&disableSslHostnameVerification&serverSslCert="
                      + serverCertPath,
                  database,
                  sslPort));
    }
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

  public static String retrieveCertificatePath() throws Exception {
    String serverCertificatePath = checkFileExists(System.getProperty("serverCertificatePath"));

    // try local server
    if (serverCertificatePath == null
        && !"skysql".equals(System.getenv("srv"))
        && !"skysql-ha".equals(System.getenv("srv"))) {
      serverCertificatePath = checkFileExists(System.getenv("TEST_DB_SERVER_CERT"));

      if (serverCertificatePath == null) {
        try (ResultSet rs = sharedConnection.createStatement().executeQuery("select @@ssl_cert")) {
          assertTrue(rs.next());
          serverCertificatePath = checkFileExists(rs.getString(1));
        }
      }
    }
    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists("../../ssl/server.crt");
    }
    return serverCertificatePath;
  }

  private static String checkFileExists(String path) throws IOException {
    if (path == null) return null;
    File f = new File(path);
    if (f.exists()) {
      return f.getCanonicalPath().replace("\\", "/");
    }
    return null;
  }
}
