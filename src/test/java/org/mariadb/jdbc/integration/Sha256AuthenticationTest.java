// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import org.junit.jupiter.api.*;

public class Sha256AuthenticationTest extends Common {

  private static String rsaPublicKey;
  private static boolean strictPasswordValidation;

  private static void dropUserWithoutError(org.mariadb.jdbc.Statement stmt, String user) {
    try {
      stmt.execute("DROP USER IF EXISTS " + user);
    } catch (SQLException e) {
      // eat
    }
  }

  @AfterAll
  public static void drop() throws SQLException {
    if (sharedConn != null) {
      org.mariadb.jdbc.Statement stmt = sharedConn.createStatement();
      dropUserWithoutError(stmt, "'cachingSha256User'" + getHostSuffix());
      dropUserWithoutError(stmt, "'cachingSha256User2'" + getHostSuffix());
      dropUserWithoutError(stmt, "'cachingSha256User3'" + getHostSuffix());
      dropUserWithoutError(stmt, "'cachingSha256User4'" + getHostSuffix());
    }
    // reason is that after nativePassword test, it sometime always return wrong authentication id
    // not cached
    // !? strange, but mysql server error.
    if (haveSsl()
        && ((isMariaDBServer() && minVersion(12, 1, 1))
            || (!isMariaDBServer() && minVersion(8, 0, 0)))) {
      try (Connection con = createCon("sslMode=trust")) {
        con.createStatement().execute("DO 1");
      }
    }
  }

  @BeforeAll
  public static void init() throws Exception {
    Assumptions.assumeTrue(
        (isMariaDBServer() && minVersion(12, 1, 1)) || (!isMariaDBServer() && minVersion(8, 0, 0)));
    drop();

    Statement stmt = sharedConn.createStatement();

    if (isMariaDBServer() && minVersion(12, 1, 1)) {
      try {
        stmt.execute("INSTALL SONAME 'auth_mysql_sha2'");
      } catch (Exception e) {
      }
    }
    rsaPublicKey = checkFileExists(System.getProperty("rsaPublicKey"));
    if (rsaPublicKey == null) {
      try {
        ResultSet rs =
            stmt.executeQuery("SELECT @@caching_sha2_password_public_key_path, @@datadir");
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
      } catch (SQLException e) {
        // eat
      }
    }
    if (rsaPublicKey == null) {
      rsaPublicKey = checkFileExists("../../ssl/public.key");
    }

    if (rsaPublicKey == null) {
      ResultSet rs = stmt.executeQuery("SHOW STATUS like 'Caching_sha2_password_rsa_public_key'");
      if (rs.next()) {
        rsaPublicKey = rs.getString(2);
        if ("".equals(rsaPublicKey)) rsaPublicKey = null;
        if (rsaPublicKey != null) {
          System.out.println(
              "rsaPublicKey set from @@Caching_sha2_password_rsa_public_key:" + rsaPublicKey);
        }
      }
    }
    try {
      ResultSet rs = stmt.executeQuery("SELECT @@global.strict_password_validation");
      rs.next();
      strictPasswordValidation = rs.getBoolean(1);
    } catch (SQLException e) {
      strictPasswordValidation = false;
    }

    String keyword = isMariaDBServer() ? "VIA" : "WITH";
    String password =
        isMariaDBServer() ? "USING PASSWORD('heyPassw-!*20oRd')" : "BY 'heyPassw-!*20oRd'";
    String passwordEmpty = isMariaDBServer() ? "USING PASSWORD('')" : "BY ''";

    stmt.execute(
        "CREATE USER 'cachingSha256User'"
            + getHostSuffix()
            + " IDENTIFIED "
            + keyword
            + " caching_sha2_password "
            + password);
    stmt.execute(
        "CREATE USER 'cachingSha256User3'"
            + getHostSuffix()
            + " IDENTIFIED "
            + keyword
            + " caching_sha2_password "
            + password);
    stmt.execute(
        "CREATE USER 'cachingSha256User4'"
            + getHostSuffix()
            + " IDENTIFIED "
            + keyword
            + " caching_sha2_password "
            + password);
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User'" + getHostSuffix());
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User3'" + getHostSuffix());
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User4'" + getHostSuffix());
    if (!strictPasswordValidation) {
      stmt.execute(
          "CREATE USER 'cachingSha256User2'"
              + getHostSuffix()
              + " IDENTIFIED "
              + keyword
              + " caching_sha2_password "
              + passwordEmpty);
      stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User2'" + getHostSuffix());
    }

    stmt.execute("FLUSH PRIVILEGES");
  }

  private static String checkFileExists(String path) throws IOException {
    if (path == null) return null;
    System.out.println("check path:" + path);
    File f = new File(path);
    if (f.exists()) {
      System.out.println("path exist :" + path);
      String returnValue = f.getCanonicalPath().replace("\\", "/");

      try {
        Files.readAllBytes(Paths.get(returnValue));
      } catch (IOException ex) {
        return null;
      }
      return returnValue;
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
      stmt.execute("DROP USER tmpUser" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }

    stmt.execute(
        "CREATE USER tmpUser"
            + getHostSuffix()
            + " IDENTIFIED WITH mysql_native_password BY 'heyPassw-!*20oRd'");
    stmt.execute("grant all on `" + sharedConn.getCatalog() + "`.* TO tmpUser" + getHostSuffix());
    stmt.execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con = createCon("user=tmpUser&password=heyPassw-!*20oRd")) {
      con.isValid(1);
    }
    try {
      stmt.execute("DROP USER tmpUser" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }
  }

  @Test
  public void cachingSha256Empty() throws Exception {
    Assumptions.assumeTrue(
        !strictPasswordValidation
            && !isWindows()
            && rsaPublicKey != null
            && ((isMariaDBServer() && minVersion(12, 1, 1))
                || (!isMariaDBServer() && minVersion(8, 0, 0))));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con = createCon("user=cachingSha256User2&allowPublicKeyRetrieval&password=")) {
      con.isValid(1);
    }
  }

  @Test
  public void wrongRsaPath() throws Exception {
    Assumptions.assumeTrue(
        !isWindows()
            && rsaPublicKey != null
            && ((isMariaDBServer() && minVersion(12, 1, 1))
                || (!isMariaDBServer() && minVersion(8, 0, 0))));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    File tempFile = File.createTempFile("log", ".tmp");
    Common.assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                "user=cachingSha256User4&serverRsaPublicKeyFile="
                    + tempFile.getPath()
                    + "2&password=heyPassw-!*20oRd"),
        "Could not read server RSA public key from file");
  }

  @Test
  public void cachingSha256Allow() throws Exception {
    Assumptions.assumeTrue(
        rsaPublicKey != null
            && ((isMariaDBServer() && minVersion(12, 1, 1))
                || (!isMariaDBServer() && minVersion(8, 0, 0))));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        createCon("user=cachingSha256User3&allowPublicKeyRetrieval&password=heyPassw-!*20oRd")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTest() throws Exception {
    Assumptions.assumeTrue(
        rsaPublicKey != null
            && ((isMariaDBServer() && minVersion(12, 1, 1))
                || (!isMariaDBServer() && minVersion(8, 0, 0))));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    try (Connection con =
        createCon(
            "user=cachingSha256User&password=heyPassw-!*20oRd&serverRsaPublicKeyFile="
                + rsaPublicKey)) {
      con.isValid(1);
    }

    try (Connection con =
        createCon("user=cachingSha256User&password=heyPassw-!*20oRd&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }

    Assumptions.assumeTrue(haveSsl());
    try (Connection con =
        createCon("user=cachingSha256User&password=heyPassw-!*20oRd&sslMode=trust")) {
      con.isValid(1);
    }

    try (Connection con =
        createCon("user=cachingSha256User&password=heyPassw-!*20oRd&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }

    try (Connection con =
        createCon(
            "user=cachingSha256User&password=heyPassw-!*20oRd&serverRsaPublicKeyFile="
                + rsaPublicKey)) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTest2() throws Exception {
    Assumptions.assumeTrue(
        ((rsaPublicKey != null && isMariaDBServer() && minVersion(12, 1, 1))
            || (!isMariaDBServer() && minVersion(8, 0, 0))));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        createCon(
            "user=cachingSha256User&password=heyPassw-!*20oRd&allowPublicKeyRetrieval&serverRsaPublicKeyFile=")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws Exception {
    Assumptions.assumeTrue(
        !isWindows()
            && ((rsaPublicKey != null && isMariaDBServer() && minVersion(12, 1, 1))
                || (!isMariaDBServer() && minVersion(8, 0, 0))));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        createCon("user=cachingSha256User&password=heyPassw-!*20oRd&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTestException() throws Exception {
    Assumptions.assumeTrue(
        (isMariaDBServer() && minVersion(12, 1, 1)) || (!isMariaDBServer() && minVersion(8, 0, 0)));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("user=cachingSha256User&password=heyPassw-!*20oRd&serverRsaPublicKeyFile="),
        "RSA public key is not available client side");
  }
}
