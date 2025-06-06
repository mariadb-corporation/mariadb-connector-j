package org.mariadb.jdbc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sun.jna.Platform;
import java.io.File;
import java.sql.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class Sha256AuthenticationTest extends BaseTest {

  private String serverPublicKey;
  private String forceTls = "";

  /**
   * Check requirement.
   *
   * @throws SQLException exception exception
   */
  @Before
  public void checkSsl() throws SQLException {
    Assume.assumeTrue(!isMariadbServer() && minVersion(5, 7));
    serverPublicKey = System.getProperty("serverPublicKey");
    // try default if not present
    if (serverPublicKey == null) {
      File sslDir = new File(System.getProperty("user.dir") + "/../ssl");
      if (sslDir.exists() && sslDir.isDirectory()) {
        serverPublicKey = System.getProperty("user.dir") + "/../ssl/public.key";
      }
    }
    Statement stmt = sharedConnection.createStatement();
    try {
      stmt.execute("DROP USER 'sha256User'" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }
    try {
      stmt.execute("DROP USER 'cachingSha256User'" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }

    if (minVersion(8, 0, 0)) {
      stmt.execute(
          "CREATE USER 'sha256User'"
              + getHostSuffix()
              + " IDENTIFIED WITH sha256_password BY 'password'");
      stmt.execute("GRANT SELECT ON *.* TO 'sha256User'" + getHostSuffix());
    } else {
      stmt.execute("CREATE USER 'sha256User'" + getHostSuffix());
      stmt.execute(
          "GRANT SELECT ON *.* TO 'sha256User'"
              + getHostSuffix()
              + " IDENTIFIED WITH "
              + "sha256_password BY 'password'");
    }
    if (minVersion(8, 0, 0)) {
      stmt.execute(
          "CREATE USER 'cachingSha256User'"
              + getHostSuffix()
              + "  IDENTIFIED WITH caching_sha2_password BY 'password'");
      stmt.execute("GRANT SELECT ON *.* TO 'cachingSha256User'" + getHostSuffix());
    } else {
      forceTls = "&enabledSslProtocolSuites=TLSv1.1,TLSv1.2";
    }
  }

  @Test
  public void sha256PluginTestWithServerRsaKey() throws SQLException {
    Assume.assumeNotNull(serverPublicKey);
    Assume.assumeTrue(minVersion(8, 0, 0));

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=sha256User&password=password&serverRsaPublicKeyFile="
                + serverPublicKey)) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void sha256PluginTestWithoutServerRsaKey() throws SQLException {
    Assume.assumeTrue(!Platform.isWindows() && minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assume.assumeTrue(!isMariadbServer() && !minVersion(8, 0, 31));

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=sha256User&password=password&allowPublicKeyRetrieval")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void sha256PluginTestException() {
    try {
      DriverManager.getConnection(
          "jdbc:mariadb://"
              + ((hostname == null) ? "localhost" : hostname)
              + ":"
              + port
              + "/"
              + ((database == null) ? "" : database)
              + "?user=sha256User&password=password");
      fail("must have throw exception");
    } catch (SQLException sqle) {
      assertTrue(sqle.getMessage().contains("RSA public key is not available client side"));
    }
  }

  @Test
  public void sha256PluginTestSsl() throws SQLException {
    Assume.assumeTrue(haveSsl(sharedConnection));
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=sha256User&password=password&useSsl&trustServerCertificate"
                + forceTls)) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void cachingSha256PluginTestWithServerRsaKey() throws SQLException {
    Assume.assumeNotNull(serverPublicKey);
    Assume.assumeTrue(minVersion(8, 0, 0));
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=cachingSha256User&password=password&serverRsaPublicKeyFile="
                + serverPublicKey)) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws SQLException {
    Assume.assumeTrue(minVersion(8, 0, 0));
    // mysql 8.0.31 broken public key retrieval, so avoid FLUSHING for now
    Assume.assumeTrue(!isMariadbServer() && !minVersion(8, 0, 31));

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=cachingSha256User&password=password&allowPublicKeyRetrieval")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void cachingSha256PluginTestException() {
    Assume.assumeTrue(minVersion(8, 0, 0));
    try {
      DriverManager.getConnection(
          "jdbc:mariadb://"
              + ((hostname == null) ? "localhost" : hostname)
              + ":"
              + port
              + "/"
              + ((database == null) ? "" : database)
              + "?user=cachingSha256User&password=password");
      fail("must have throw exception");
    } catch (SQLException sqle) {
      assertTrue(sqle.getMessage().contains("RSA public key is not available client side"));
    }
  }

  @Test
  public void cachingSha256PluginTestSsl() throws SQLException {
    Assume.assumeTrue(haveSsl(sharedConnection));
    Assume.assumeTrue(minVersion(8, 0, 0));
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=cachingSha256User&password=password&useSsl&trustServerCertificate=true")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }
}
