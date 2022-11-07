// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import org.junit.jupiter.api.*;

public class Sha256AuthenticationTest extends Common {

  private static String rsaPublicKey;

  private static void dropUserWithoutError(org.mariadb.jdbc.Statement stmt, String user) {
    try {
      stmt.execute("DROP USER " + user);
    } catch (SQLException e) {
      // eat
    }
  }

  @AfterAll
  public static void drop() throws SQLException {
    if (sharedConn != null) {
      org.mariadb.jdbc.Statement stmt = sharedConn.createStatement();
      dropUserWithoutError(stmt, "'cachingSha256User'@'%'");
      dropUserWithoutError(stmt, "'cachingSha256User2'@'%'");
      dropUserWithoutError(stmt, "'cachingSha256User3'@'%'");
      dropUserWithoutError(stmt, "'cachingSha256User4'@'%'");
    }
    // reason is that after nativePassword test, it sometime always return wrong authentication id
    // not cached
    // !? strange, but mysql server error.
    if (haveSsl() && !isMariaDBServer() && minVersion(8, 0, 0)) {
      try (Connection con = createCon("sslMode=trust")) {
        con.createStatement().execute("DO 1");
      }
    }
  }

  @BeforeAll
  public static void init() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));
    drop();
    Statement stmt = sharedConn.createStatement();
    rsaPublicKey = checkFileExists(System.getProperty("rsaPublicKey"));
    if (rsaPublicKey == null) {
      ResultSet rs = stmt.executeQuery("SELECT @@caching_sha2_password_public_key_path, @@datadir");
      rs.next();
      rsaPublicKey = checkFileExists(rs.getString(1));

      if (rsaPublicKey == null) {
        rsaPublicKey = checkFileExists(rs.getString(2) + rs.getString(1));
        if (rsaPublicKey == null) {
          rsaPublicKey = checkFileExists(System.getenv("TEST_DB_RSA_PUBLIC_KEY"));
          if (rsaPublicKey == null && System.getenv("TEST_DB_RSA_PUBLIC_KEY") != null) {
            rsaPublicKey = checkFileExists(System.getenv("TEST_DB_RSA_PUBLIC_KEY"));
          }
        }
      }
    }
    if (rsaPublicKey == null) {
      rsaPublicKey = checkFileExists("../../ssl/public.key");
    }

    stmt.execute(
        "CREATE USER 'cachingSha256User'@'%' IDENTIFIED WITH caching_sha2_password BY '!Passw0rd3Works'");
    stmt.execute(
        "CREATE USER 'cachingSha256User2'@'%' IDENTIFIED WITH caching_sha2_password BY ''");
    stmt.execute(
        "CREATE USER 'cachingSha256User3'@'%' IDENTIFIED WITH caching_sha2_password BY '!Passw0rd3Works'");
    stmt.execute(
        "CREATE USER 'cachingSha256User4'@'%' IDENTIFIED WITH caching_sha2_password BY '!Passw0rd3Works'");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User'@'%'");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User2'@'%'");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User3'@'%'");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User4'@'%'");
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    stmt.execute("FLUSH PRIVILEGES");
  }

  private static String checkFileExists(String path) throws IOException {
    if (path == null) return null;
    System.out.println("check path:" + path);
    File f = new File(path);
    if (f.exists()) {
      System.out.println("path exist :" + path);
      return f.getCanonicalPath().replace("\\", "/");
    }
    return null;
  }

  @Test
  public void nativePassword() throws Exception {
    Assumptions.assumeTrue(haveSsl());
    Assumptions.assumeTrue(
        !isWindows() && !isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("DROP USER tmpUser@'%'");
    } catch (SQLException e) {
      // eat
    }

    stmt.execute(
        "CREATE USER tmpUser@'%' IDENTIFIED WITH mysql_native_password BY '!Passw0rd3Works'");
    stmt.execute("grant all on `" + sharedConn.getCatalog() + "`.* TO tmpUser@'%'");
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));

    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    stmt.execute("FLUSH PRIVILEGES");

    try (Connection con = createCon("user=tmpUser&password=!Passw0rd3Works")) {
      con.isValid(1);
    }
    try {
      stmt.execute("DROP USER tmpUser@'%' ");
    } catch (SQLException e) {
      // eat
    }
  }

  @Test
  public void cachingSha256Empty() throws Exception {
    Assumptions.assumeTrue(
        !isWindows() && !isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    try (Connection con = createCon("user=cachingSha256User2&allowPublicKeyRetrieval&password=")) {
      con.isValid(1);
    }
  }

  @Test
  public void wrongRsaPath() throws Exception {
    Assumptions.assumeTrue(
        !isWindows() && !isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    File tempFile = File.createTempFile("log", ".tmp");
    Common.assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                "user=cachingSha256User4&serverRsaPublicKeyFile="
                    + tempFile.getPath()
                    + "2&password=!Passw0rd3Works"),
        "Could not read server RSA public key from file");
  }

  @Test
  public void cachingSha256Allow() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        createCon("user=cachingSha256User3&allowPublicKeyRetrieval&password=!Passw0rd3Works")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTest() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    try (Connection con =
        createCon(
            "user=cachingSha256User&password=!Passw0rd3Works&serverRsaPublicKeyFile="
                + rsaPublicKey)) {
      con.isValid(1);
    }

    try (Connection con =
        createCon("user=cachingSha256User&password=!Passw0rd3Works&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }

    Assumptions.assumeTrue(haveSsl());
    try (Connection con =
        createCon("user=cachingSha256User&password=!Passw0rd3Works&sslMode=trust")) {
      con.isValid(1);
    }

    try (Connection con =
        createCon("user=cachingSha256User&password=!Passw0rd3Works&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }

    try (Connection con =
        createCon(
            "user=cachingSha256User&password=!Passw0rd3Works&serverRsaPublicKeyFile="
                + rsaPublicKey)) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws Exception {
    Assumptions.assumeTrue(!isWindows() && minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        createCon("user=cachingSha256User&password=!Passw0rd3Works&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTestException() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assumptions.assumeTrue(!isMariaDBServer() && !exactVersion(8, 0, 31));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("user=cachingSha256User&password=!Passw0rd3Works"),
        "RSA public key is not available client side");
  }
}
