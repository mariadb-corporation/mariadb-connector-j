// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.*;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.integration.tools.TcpProxy;
import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Locale;
import org.junit.jupiter.api.*;

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
        System.getenv("TEST_MAXSCALE_TLS_PORT") == null
                || System.getenv("TEST_MAXSCALE_TLS_PORT").isEmpty()
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
    try (Connection con = createCon("sslMode=trust&useReadAheadInput=false", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
  }

  @Test
  public void mandatorySsl() throws SQLException {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con = createCon(baseOptions + "&sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrows(SQLException.class, () -> createCon(baseOptions + "&sslMode=disable"));
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () -> createCon(baseMutualOptions + "&sslMode=trust", sslPort));
  }

  @Test
  public void enabledSslProtocolSuites() throws SQLException {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con =
        createCon(
            baseOptions + "&sslMode=trust&enabledSslProtocolSuites=TLSv1.2,TLSv1.3", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () ->
            createCon(baseMutualOptions + "&sslMode=trust&enabledSslProtocolSuites=SSLv3", sslPort),
        "No appropriate protocol");
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseMutualOptions + "&sslMode=trust&enabledSslProtocolSuites=unknown", sslPort),
        "Unsupported SSL protocol 'unknown'");
  }

  @Test
  public void enabledSslCipherSuites() throws SQLException {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=trust&enabledSslCipherSuites=TLS_DHE_RSA_WITH_AES_256_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseMutualOptions + "&sslMode=trust&enabledSslCipherSuites=UNKNOWN_CIPHER",
                sslPort),
        "Unsupported SSL cipher");
  }

  @Test
  public void mutualAuthSsl() throws SQLException {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(System.getenv("TEST_DB_CLIENT_PKCS") != null);

    // without password
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            createCon(
                baseMutualOptions
                    + "&sslMode=trust&keyStore="
                    + System.getenv("TEST_DB_CLIENT_PKCS"),
                sslPort));
    // with password
    try (Connection con =
        createCon(
            baseMutualOptions
                + "&sslMode=trust&keyStore="
                + System.getenv("TEST_DB_CLIENT_PKCS")
                + "&keyStorePassword=kspass",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // with URL
    boolean isWin = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
    try (Connection con =
        createCon(
            baseMutualOptions
                + "&sslMode=trust&keyStore="
                + "file://"
                + (isWin ? "/" : "")
                + System.getenv("TEST_DB_CLIENT_PKCS")
                + "&keyStorePassword=kspass",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // wrong keystore type
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            createCon(
                baseMutualOptions
                    + "&sslMode=trust&keyStoreType=JKS&keyStore="
                    + System.getenv("TEST_DB_CLIENT_PKCS"),
                sslPort));
    // good keystore type
    try (Connection con =
        createCon(
            baseMutualOptions
                + "&sslMode=trust&keyStoreType=pkcs12&keyStore="
                + System.getenv("TEST_DB_CLIENT_PKCS")
                + "&keyStorePassword=kspass",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // with system properties
    System.setProperty("javax.net.ssl.keyStore", System.getenv("TEST_DB_CLIENT_PKCS"));
    System.setProperty("javax.net.ssl.keyStorePassword", "kspass");
    try (Connection con = createCon(baseMutualOptions + "&sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // wrong keystore type
    System.setProperty("javax.net.ssl.keyStoreType", "JKS");
    try (Connection con = createCon(baseMutualOptions + "&sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con = createCon(baseMutualOptions + "&sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    System.clearProperty("javax.net.ssl.keyStoreType");
    try (Connection con =
        createCon(baseMutualOptions + "&sslMode=trust&keyStoreType=JKS", sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // without password
    System.clearProperty("javax.net.ssl.keyStorePassword");
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () -> createCon(baseMutualOptions + "&sslMode=trust", sslPort));
  }

  @Test
  public void certificateMandatorySsl() throws Throwable {

    String serverCertPath = retrieveCertificatePath();
    Assumptions.assumeTrue(serverCertPath != null, "Canceled, server certificate not provided");

    // certificate path, like  /path/certificate.crt
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=" + serverCertPath, sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    if (!"localhost".equals(hostname)) {
      try (Connection con =
          createCon(
              baseOptions + "&sslMode=VERIFY_FULL&serverSslCert=" + serverCertPath, sslPort)) {
        assertNotNull(getSslVersion(con));
      }

      Configuration conf = Configuration.parse(mDefUrl);
      HostAddress hostAddress = conf.addresses().get(0);
      try {
        proxy = new TcpProxy(hostAddress.host, sslPort == null ? hostAddress.port : sslPort);
      } catch (IOException i) {
        throw new SQLException("proxy error", i);
      }

      String url = mDefUrl.replaceAll("//([^/]*)/", "//localhost:" + proxy.getLocalPort() + "/");
      assertThrowsContains(
          SQLException.class,
          () ->
              DriverManager.getConnection(
                  url + "&sslMode=VERIFY_FULL&serverSslCert=" + serverCertPath),
          "DNS host \"localhost\" doesn't correspond to certificate");
    }

    String urlPath = Paths.get(serverCertPath).toUri().toURL().toString();
    // file certificate path, like  file:/path/certificate.crt
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=" + urlPath, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    String certificateString = getServerCertificate(serverCertPath);
    // file certificate, like  -----BEGIN CERTIFICATE-----...
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=" + certificateString, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    assertThrows(
        SQLNonTransientConnectionException.class,
        () -> createCon(baseOptions + "&sslMode=VERIFY_CA", sslPort));
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () ->
            createCon(
                baseMutualOptions + "&sslMode=VERIFY_CA&serverSslCert=" + serverCertPath, sslPort));
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

  private static String checkFileExists(String path) throws IOException {
    if (path == null) return null;
    File f = new File(path);
    if (f.exists()) {
      return f.getCanonicalPath().replace("\\", "/");
    }
    return null;
  }
}
