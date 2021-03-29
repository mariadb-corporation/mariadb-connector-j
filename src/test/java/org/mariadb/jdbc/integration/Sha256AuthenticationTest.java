package org.mariadb.jdbc.integration;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;

public class Sha256AuthenticationTest extends Common {

  private static String rsaPublicKey;
  private static boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");

  @AfterAll
  public static void drop() throws SQLException {
    if (sharedConn != null) {
      org.mariadb.jdbc.Statement stmt = sharedConn.createStatement();
      stmt.execute("DROP USER IF EXISTS 'cachingSha256User'@'%'");
      stmt.execute("DROP USER IF EXISTS 'cachingSha256User2'@'%'");
    }
  }

  @BeforeAll
  public static void init() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));
    drop();
    Statement stmt = sharedConn.createStatement();
    rsaPublicKey = checkFileExists(System.getProperty("rsaPublicKey"));
    if (rsaPublicKey == null) {
      ResultSet rs = stmt.executeQuery("SELECT @@caching_sha2_password_public_key_path");
      rs.next();
      rsaPublicKey = checkFileExists(rs.getString(1));
      if (rsaPublicKey == null
          && rs.getString(1) != null
          && System.getenv("TEST_DB_RSA_PUBLIC_KEY") != null) {
        rsaPublicKey = checkFileExists(System.getenv("TEST_DB_RSA_PUBLIC_KEY"));
      }
    }
    if (rsaPublicKey == null) {
      rsaPublicKey = checkFileExists("../../ssl/public.key");
    }

    stmt.execute(
        "CREATE USER 'cachingSha256User'@'%' IDENTIFIED WITH caching_sha2_password BY 'MySup8rPassw@ord'");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User'@'%'");
    stmt.execute(
        "CREATE USER 'cachingSha256User2'@'%' IDENTIFIED WITH caching_sha2_password BY ''");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User2'@'%'");
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
    Assumptions.assumeTrue(
        !isWindows && !isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS tmpUser@'%'");
    stmt.execute(
        "CREATE USER tmpUser@'%' IDENTIFIED WITH mysql_native_password BY '!Passw0rd3Works'");
    stmt.execute("grant all on `" + sharedConn.getCatalog() + "`.* TO tmpUser@'%'");
    stmt.execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con = createCon("user=tmpUser&password=!Passw0rd3Works")) {
      con.isValid(1);
    } catch (SQLException sqle) {
      // mysql authentication might fail !?
    }
    stmt.execute("DROP USER IF EXISTS tmpUser@'%' ");
  }

  @Test
  public void cachingSha256Empty() throws Exception {
    Assumptions.assumeTrue(
        !isWindows && !isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con = createCon("user=cachingSha256User2&allowPublicKeyRetrieval&password=")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTest() throws Exception {
    Assumptions.assumeTrue(
        !isWindows && !isMariaDBServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    try (Connection con =
        createCon(
            "user=cachingSha256User&password=MySup8rPassw@ord&serverRsaPublicKeyFile="
                + rsaPublicKey)) {
      con.isValid(1);
    } catch (SQLException sqle) {
      // mysql authentication might fail !?
    }

    try (Connection con =
                 createCon("user=cachingSha256User&password=MySup8rPassw@ord&allowPublicKeyRetrieval")) {
      con.isValid(1);
    } catch (SQLException sqle) {
      // mysql authentication might fail !?
    }

    Assumptions.assumeTrue(haveSsl());
    try (Connection con =
                 createCon("user=cachingSha256User&password=MySup8rPassw@ord&sslMode=trust")) {
      con.isValid(1);
    }

    try (Connection con =
                 createCon("user=cachingSha256User&password=MySup8rPassw@ord&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }

    try (Connection con =
                 createCon(
                         "user=cachingSha256User&password=MySup8rPassw@ord&serverRsaPublicKeyFile="
                                 + rsaPublicKey)) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws Exception {
    Assumptions.assumeTrue(!isWindows && minVersion(8, 0, 0));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        createCon("user=cachingSha256User&password=MySup8rPassw@ord&allowPublicKeyRetrieval")) {
      con.isValid(1);
    } catch (SQLException sqle) {
      // mysql authentication might fail !?
    }
  }

  @Test
  public void cachingSha256PluginTestException() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));
    sharedConn.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    assertThrowsContains(
        SQLException.class,
        () -> createCon("user=cachingSha256User&password=MySup8rPassw@ord"),
        "RSA public key is not available client side");
  }

}
