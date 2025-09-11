// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.sql.*;
import java.util.*;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.integration.tools.TcpProxy;

@DisplayName("SSL tests")
public class SslTest extends Common {
  private static final String baseOptions = "&user=serverAuthUser&password=!Passw0rd3Works";
  private static final String baseMutualOptions = "&user=mutualAuthUser&password=!Passw0rd3Works";
  private static Integer sslPort;

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("DROP USER serverAuthUser");
    } catch (SQLException e) {
      // eat
    }
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
    boolean useOldNotation =
        (!isMariaDBServer() || !minVersion(10, 2, 0))
            && (isMariaDBServer() || !minVersion(8, 0, 0));
    Statement stmt = sharedConn.createStatement();
    if (useOldNotation) {
      stmt.execute(
          "CREATE USER IF NOT EXISTS '" + user + "'" + getHostSuffix() + " " + requirement);
      stmt.execute(
          "GRANT SELECT ON *.* TO '"
              + user
              + "'"
              + getHostSuffix()
              + " IDENTIFIED BY '!Passw0rd3Works' "
              + requirement);
    } else {
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        stmt.execute(
            "CREATE USER IF NOT EXISTS '"
                + user
                + "'"
                + getHostSuffix()
                + " IDENTIFIED WITH mysql_native_password BY '!Passw0rd3Works' "
                + requirement);
      } else {
        stmt.execute(
            "CREATE USER IF NOT EXISTS '"
                + user
                + "'"
                + getHostSuffix()
                + " IDENTIFIED BY '!Passw0rd3Works' "
                + requirement);
      }
      stmt.execute(
          "GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO '" + user + "'" + getHostSuffix());
    }
  }

  public static String retrieveCertificatePath() throws Exception {
    String serverCertificatePath = checkFileExists(System.getProperty("serverCertificatePath"));
    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists(System.getenv("TEST_DB_SERVER_CERT"));
    }

    // try local server
    if (serverCertificatePath == null) {

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

  public static String retrieveCaCertificatePath() throws Exception {
    String serverCertificatePath = checkFileExists(System.getProperty("serverCertificatePath"));
    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists(System.getenv("TEST_DB_SERVER_CA_CERT"));
    }

    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists("../../ssl/ca.crt");
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

  private String getSslVersion(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    if (isMaxscale()) {
      return "ok";
    } else {
      ResultSet rs = stmt.executeQuery("show STATUS  LIKE 'Ssl_version'");
      if (rs.next()) {
        return rs.getString(2);
      }
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
    Assumptions.assumeTrue(!isMaxscale());
    try (Connection con = createCon(baseOptions + "&sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrows(SQLException.class, () -> createCon(baseOptions + "&sslMode=disable"));
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () -> createCon(baseMutualOptions + "&sslMode=trust", sslPort));
  }

  @Test
  public void mandatoryEphemeralSsl() throws SQLException {
    Assumptions.assumeTrue(!isMaxscale());
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(11, 4, 1));
    try (Connection con = createCon(baseOptions + "&sslMode=verify-ca", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con = createCon(baseOptions + "&sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con = createCon(baseOptions + "&sslMode=verify-full", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrows(SQLException.class, () -> createCon(baseOptions + "&sslMode=disable"));
    assertThrows(
        SQLInvalidAuthorizationSpecException.class,
        () -> createCon(baseMutualOptions + "&sslMode=trust", sslPort));
  }

  @Test
  void ensureSslUnixSocket() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(11, 4, 1));
    Assumptions.assumeTrue(
        System.getenv("local") != null
            && "1".equals(System.getenv("local"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    java.sql.Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select @@version_compile_os,@@socket");
    if (!rs.next() || rs.getString(2) == null) {
      return;
    }
    String path = rs.getString(2);

    try (java.sql.Connection connectionSocket =
        createCon(baseOptions + "&sslMode=verify-ca&localSocket=" + path, sslPort)) {
      ResultSet resultSet = connectionSocket.createStatement().executeQuery("select 1");
      assertTrue(resultSet.next());
    }
  }

  @Test
  public void mandatoryEphemeralSsled25519() throws SQLException {
    Assumptions.assumeTrue(!isMaxscale());
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(11, 4, 1));

    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("INSTALL SONAME 'auth_ed25519'");
    } catch (SQLException sqle) {
      Assumptions.assumeTrue(false, "server doesn't have ed25519 plugin, cancelling test");
    }
    try {
      stmt.execute("drop user if exists verificationEd25519AuthPlugin" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }
    stmt.execute(
        "CREATE USER IF NOT EXISTS verificationEd25519AuthPlugin"
            + getHostSuffix()
            + " IDENTIFIED "
            + "VIA ed25519 USING PASSWORD('MySup8%rPassw@ord') REQUIRE SSL");
    stmt.execute(
        "GRANT SELECT ON "
            + sharedConn.getCatalog()
            + ".* TO verificationEd25519AuthPlugin"
            + getHostSuffix());
    try (Connection con =
        createCon(
            "user=verificationEd25519AuthPlugin&password=MySup8%rPassw@ord&sslMode=verify-ca",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con =
        createCon(
            "user=verificationEd25519AuthPlugin&password=MySup8%rPassw@ord&sslMode=trust",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    stmt.execute("drop user if exists verificationEd25519AuthPlugin@'%'");
  }

  @Test
  public void mandatoryEphemeralSslParsec() throws SQLException {
    Assumptions.assumeTrue(!isMaxscale());
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(11, 6, 1) && getJavaVersion() >= 15);

    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("INSTALL SONAME 'auth_parsec'");
    } catch (SQLException sqle) {
      Assumptions.assumeTrue(false, "server doesn't have auth_parsec plugin, cancelling test");
    }
    try {
      stmt.execute("drop user if exists verificationParsecAuthPlugin" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }
    stmt.execute(
        "CREATE USER IF NOT EXISTS verificationParsecAuthPlugin"
            + getHostSuffix()
            + " IDENTIFIED "
            + "VIA parsec USING PASSWORD('MySup8%rPassw@ord') REQUIRE SSL");
    stmt.execute(
        "GRANT SELECT ON "
            + sharedConn.getCatalog()
            + ".* TO verificationParsecAuthPlugin"
            + getHostSuffix());
    try (Connection con =
        createCon(
            "user=verificationParsecAuthPlugin&password=MySup8%rPassw@ord&sslMode=verify-ca",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con =
        createCon(
            "user=verificationParsecAuthPlugin&password=MySup8%rPassw@ord&sslMode=trust",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    stmt.execute("drop user if exists verificationParsecAuthPlugin@'%'");
  }

  @Test
  void ensureUnixSocketSsl() throws SQLException {
    Assumptions.assumeTrue(
        System.getenv("local") != null
            && "1".equals(System.getenv("local"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    java.sql.Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select @@version_compile_os,@@socket");
    if (!rs.next() || rs.getString(2) == null) {
      return;
    }
    String path = rs.getString(2);
    String url =
        mDefUrl.replaceAll(
            "//(" + hostname + "|" + hostname + ":" + port + ")/",
            "//address=(localSocket="
                + path
                + "),address=(host="
                + hostname
                + ")(port="
                + port
                + ")(sslMode=verify-full)(type=primary)/");
    try (Connection con = (Connection) DriverManager.getConnection(url)) {
      assertNotNull(getSslVersion(con));
      Assertions.assertEquals("address=(localSocket=" + path + ")", con.__test_host());
    }
  }

  @Test
  public void enabledSslProtocolSuites() throws SQLException {
    Assumptions.assumeTrue(!isMaxscale());
    try {
      List<String> protocols =
          Arrays.asList(SSLContext.getDefault().getSupportedSSLParameters().getProtocols());
      Assumptions.assumeTrue(protocols.contains("TLSv1.3") && protocols.contains("TLSv1.2"));
    } catch (NoSuchAlgorithmException e) {
      // eat
    }
    try (Connection con =
        createCon(
            baseOptions + "&sslMode=trust&enabledSslProtocolSuites=TLSv1.2,TLSv1.3", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    Common.assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () ->
            createCon(baseMutualOptions + "&sslMode=trust&enabledSslProtocolSuites=SSLv3", sslPort),
        "No appropriate protocol");
    Common.assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseMutualOptions + "&sslMode=trust&enabledSslProtocolSuites=unknown", sslPort),
        "Unsupported SSL protocol 'unknown'");
  }

  @Test
  public void enabledSslCipherSuites() throws SQLException {
    Assumptions.assumeTrue(!isMaxscale());
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=trust&enabledSslCipherSuites=TLS_DHE_RSA_WITH_AES_256_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    Common.assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseMutualOptions + "&sslMode=trust&enabledSslCipherSuites=UNKNOWN_CIPHER",
                sslPort),
        "Unsupported SSL cipher");
  }

  @Test
  public void errorUsingWrongTypeOfKeystore() throws Exception {
    Assumptions.assumeTrue(!isMaxscale());
    String pkcsFile = System.getenv("TEST_DB_CLIENT_PKCS");
    Assumptions.assumeTrue(pkcsFile != null);

    if (checkFileExists(pkcsFile) != null) {
      // wrong keystore type
      assertThrows(
          SQLNonTransientConnectionException.class,
          () ->
              createCon(
                  baseMutualOptions
                      + "&sslMode=verify-ca&serverSslCert="
                      + pkcsFile
                      + "&trustStoreType=JKS&keyStore="
                      + System.getenv("TEST_DB_CLIENT_PKCS")
                      + "&keyStorePassword=kspass",
                  sslPort));
    }
  }

  @Test
  public void mutualAuthSsl() throws Exception {
    Assumptions.assumeTrue(!isMaxscale());
    Assumptions.assumeTrue(
        System.getenv("TEST_DB_CLIENT_PKCS") != null
            && !"".equals(System.getenv("TEST_DB_CLIENT_PKCS")));
    System.out.println("TEST_DB_CLIENT_PKCS:" + System.getenv("TEST_DB_CLIENT_PKCS"));

    // without password
    try {
      assertThrows(
          SQLInvalidAuthorizationSpecException.class,
          () ->
              createCon(
                  baseMutualOptions
                      + "&sslMode=trust&keyStore="
                      + System.getenv("TEST_DB_CLIENT_PKCS"),
                  sslPort));
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
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

    String prevValue = System.getProperty("javax.net.ssl.keyStore");
    String prevPwdValue = System.getProperty("javax.net.ssl.keyStorePassword");
    System.setProperty(
        "javax.net.ssl.keyStore",
        "file://" + (isWin ? "/" : "") + System.getenv("TEST_DB_CLIENT_PKCS"));
    System.setProperty("javax.net.ssl.keyStorePassword", "kspass");
    try {
      try (Connection con = createCon(baseMutualOptions + "&sslMode=trust", sslPort)) {
        assertNotNull(getSslVersion(con));
      }
      assertThrows(
          SQLInvalidAuthorizationSpecException.class,
          () ->
              createCon(
                  baseMutualOptions + "&sslMode=trust&fallbackToSystemKeyStore=false", sslPort));
    } finally {
      if (prevValue == null) {
        System.clearProperty("javax.net.ssl.keyStore");
      } else {
        System.setProperty("javax.net.ssl.keyStore", prevValue);
      }
      if (prevPwdValue == null) {
        System.clearProperty("javax.net.ssl.keyStorePassword");
      } else {
        System.setProperty("javax.net.ssl.keyStorePassword", prevPwdValue);
      }
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
    Assumptions.assumeTrue(!isMaxscale());
    String serverCertPath = retrieveCertificatePath();
    Assumptions.assumeTrue(serverCertPath != null, "Canceled, server certificate not provided");

    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon(baseOptions + "&sslMode=VERIFY_CA&fallbackToSystemTrustStore=false"),
        "No X509TrustManager found");

    // certificate path, like  /path/certificate.crt
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=" + serverCertPath, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=file:///wrongPath", sslPort)) {
      assertNotNull(getSslVersion(con));
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IOException);
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

      String url =
          mDefUrl.replaceAll(
              "//(" + hostname + "|" + hostname + ":" + port + ")/",
              "//localhost:" + proxy.getLocalPort() + "/");
      Common.assertThrowsContains(
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

    if (isMariaDBServer() && minVersion(11, 4, 1)) {
      try (Connection conn = createBasicCon(baseOptions + "&sslMode=VERIFY_CA", sslPort)) {
        conn.isValid(1);
      }
    } else {
      try {
        createBasicCon(baseOptions + "&sslMode=VERIFY_CA", sslPort);
        fail("must have thrown error");
      } catch (Exception e) {
        assertTrue(
            e.getMessage().contains("unable to find valid certification")
                || e.getMessage().contains("Self signed certificate"));
      }
    }

    if (!"maxscale".equals(System.getenv("srv"))) {
      assertThrows(
          SQLInvalidAuthorizationSpecException.class,
          () ->
              createBasicCon(
                  baseMutualOptions + "&sslMode=VERIFY_CA&serverSslCert=" + serverCertPath,
                  sslPort));
    }
  }

  @Test
  public void trustStoreParameter() throws Throwable {
    Assumptions.assumeTrue(!isMaxscale());
    String serverCertPath = retrieveCertificatePath();
    String caCertPath = retrieveCaCertificatePath();
    Assumptions.assumeTrue(serverCertPath != null, "Canceled, server certificate not provided");

    KeyStore ks = KeyStore.getInstance("jks");
    char[] pwdArray = "myPwd0".toCharArray();
    ks.load(null, pwdArray);

    File temptrustStoreFile = File.createTempFile("newKeyStoreFileName", ".jks");

    KeyStore ks2 = KeyStore.getInstance("pkcs12");
    ks2.load(null, pwdArray);
    File temptrustStoreFile2 = File.createTempFile("newKeyStoreFileName", ".pkcs12");

    try (InputStream inStream = new File(serverCertPath).toURI().toURL().openStream()) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Collection<? extends Certificate> serverCertList = cf.generateCertificates(inStream);
      List<Certificate> certs = new ArrayList<>();
      for (Iterator<? extends Certificate> iter = serverCertList.iterator(); iter.hasNext(); ) {
        certs.add(iter.next());
      }
      if (caCertPath != null) {
        try (InputStream inStream2 = new File(caCertPath).toURI().toURL().openStream()) {
          CertificateFactory cf2 = CertificateFactory.getInstance("X.509");
          Collection<? extends Certificate> caCertList = cf2.generateCertificates(inStream2);
          for (Iterator<? extends Certificate> iter = caCertList.iterator(); iter.hasNext(); ) {
            certs.add(iter.next());
          }
        }
      }
      for (Certificate cert : certs) {
        ks.setCertificateEntry(hostname, cert);
        ks2.setCertificateEntry(hostname, cert);
      }

      try (FileOutputStream fos = new FileOutputStream(temptrustStoreFile.getPath())) {
        ks.store(fos, pwdArray);
      }
      try (FileOutputStream fos = new FileOutputStream(temptrustStoreFile2.getPath())) {
        ks2.store(fos, pwdArray);
      }
    }

    // certificate path, like  /path/certificate.crt
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=VERIFY_CA&trustStore="
                + temptrustStoreFile
                + "&trustStoreType=jks&trustStorePassword=myPwd0",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // with alias
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=VERIFY_CA&trustCertificateKeystoreUrl="
                + temptrustStoreFile
                + "&trustCertificateKeyStoretype=jks&trustCertificateKeystorePassword=myPwd0",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseOptions
                    + "&sslMode=VERIFY_CA&trustStore="
                    + temptrustStoreFile
                    + "&trustStoreType=jks&trustStorePassword=wrongPwd",
                sslPort),
        "Failed load keyStore");
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseOptions
                    + "&sslMode=VERIFY_CA&trustCertificateKeystoreUrl="
                    + temptrustStoreFile
                    + "&trustCertificateKeyStoretype=jks&trustCertificateKeystorePassword=wrongPwd",
                sslPort),
        "Failed load keyStore");
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=VERIFY_CA&trustStore="
                + temptrustStoreFile2
                + "&trustStoreType=pkcs12&trustStorePassword=myPwd0",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseOptions
                    + "&sslMode=VERIFY_CA&trustStore="
                    + temptrustStoreFile2
                    + "&trustStoreType=pkcs12&trustStorePassword=wrongPwd",
                sslPort),
        "Failed load keyStore");
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
}
