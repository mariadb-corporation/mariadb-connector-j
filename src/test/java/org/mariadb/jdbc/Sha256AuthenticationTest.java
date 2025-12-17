package org.mariadb.jdbc;

import static org.junit.Assert.*;

import com.sun.jna.Platform;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import org.junit.*;

public class Sha256AuthenticationTest extends BaseTest {

  private static String rsaPublicKey;

  private static void dropUserWithoutError(Statement stmt, String user) {
    try {
      stmt.execute("DROP USER IF EXISTS " + user);
    } catch (SQLException e) {
      // eat
    }
  }

  @After
  public void drop() throws SQLException {
    if (sharedConnection != null) {
      Statement stmt = sharedConnection.createStatement();
      dropUserWithoutError(stmt, "'cachingSha256User'" + getHostSuffix());
      dropUserWithoutError(stmt, "'cachingSha256User2'" + getHostSuffix());
      dropUserWithoutError(stmt, "'cachingSha256User3'" + getHostSuffix());
      dropUserWithoutError(stmt, "'cachingSha256User4'" + getHostSuffix());
    }
  }

  @Before
  public void initClass() throws Exception {
    Assume.assumeTrue(
        (isMariadbServer() && minVersion(12, 1, 1)) || (!isMariadbServer() && minVersion(8, 0, 0)));
    drop();

    Statement stmt = sharedConnection.createStatement();

    if (isMariadbServer() && minVersion(12, 1, 1)) {
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
    String keyword = isMariadbServer() ? "VIA" : "WITH";
    String password =
        isMariadbServer() ? "USING PASSWORD('!Passw0rd3Works')" : "BY '!Passw0rd3Works'";
    String passwordEmpty = isMariadbServer() ? "USING PASSWORD('')" : "BY ''";

    stmt.execute(
        "CREATE USER 'cachingSha256User'"
            + getHostSuffix()
            + " IDENTIFIED "
            + keyword
            + " caching_sha2_password "
            + password);
    stmt.execute(
        "CREATE USER 'cachingSha256User2'"
            + getHostSuffix()
            + " IDENTIFIED "
            + keyword
            + " caching_sha2_password "
            + passwordEmpty);
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
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User2'" + getHostSuffix());
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User3'" + getHostSuffix());
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User4'" + getHostSuffix());
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
    Assume.assumeTrue(haveSsl(sharedConnection));
    Assume.assumeTrue(
        !Platform.isWindows() && !isMariadbServer() && rsaPublicKey != null && minVersion(8, 0, 0));
    Statement stmt = sharedConnection.createStatement();
    try {
      stmt.execute("DROP USER tmpUser" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }

    stmt.execute(
        "CREATE USER tmpUser"
            + getHostSuffix()
            + " IDENTIFIED WITH mysql_native_password BY '!Passw0rd3Works'");
    stmt.execute(
        "grant all on `" + sharedConnection.getCatalog() + "`.* TO tmpUser" + getHostSuffix());
    stmt.execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con = setConnection("&user=tmpUser&password=!Passw0rd3Works")) {
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
    Assume.assumeTrue(
        !Platform.isWindows()
            && rsaPublicKey != null
            && ((isMariadbServer() && minVersion(12, 1, 1))
                || (!isMariadbServer() && minVersion(8, 0, 0))));
    sharedConnection.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        setConnection("&user=cachingSha256User2&allowPublicKeyRetrieval&password=")) {
      con.isValid(1);
    }
  }

  @Test
  public void wrongRsaPath() throws Exception {
    Assume.assumeTrue(
        !Platform.isWindows()
            && rsaPublicKey != null
            && ((isMariadbServer() && minVersion(12, 1, 1))
                || (!isMariadbServer() && minVersion(8, 0, 0))));
    sharedConnection.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    File tempFile = File.createTempFile("log", ".tmp");
    assertThrows(
        SQLException.class,
        () ->
            setConnection(
                "&user=cachingSha256User4&serverRsaPublicKeyFile="
                    + tempFile.getPath()
                    + "2&password=!Passw0rd3Works"));
  }

  @Test
  public void cachingSha256Allow() throws Exception {
    Assume.assumeTrue(
        rsaPublicKey != null
            && ((isMariadbServer() && minVersion(12, 1, 1))
                || (!isMariadbServer() && minVersion(8, 0, 0))));
    sharedConnection.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        setConnection(
            "&user=cachingSha256User3&allowPublicKeyRetrieval&password=!Passw0rd3Works")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTest() throws Exception {
    Assume.assumeTrue(
        rsaPublicKey != null
            && ((isMariadbServer() && minVersion(12, 1, 1))
                || (!isMariadbServer() && minVersion(8, 0, 0))));
    sharedConnection.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    try (Connection con =
        setConnection(
            "&user=cachingSha256User&password=!Passw0rd3Works&serverRsaPublicKeyFile="
                + rsaPublicKey)) {
      con.isValid(1);
    }

    try (Connection con =
        setConnection("&user=cachingSha256User&password=!Passw0rd3Works&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }

    Assume.assumeTrue(haveSsl(sharedConnection));
    try (Connection con =
        setConnection(
            "&user=cachingSha256User&password=!Passw0rd3Works&useSsl=true&trustServerCertificate")) {
      con.isValid(1);
    }

    try (Connection con =
        setConnection("&user=cachingSha256User&password=!Passw0rd3Works&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }

    try (Connection con =
        setConnection(
            "&user=cachingSha256User&password=!Passw0rd3Works&serverRsaPublicKeyFile="
                + rsaPublicKey)) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTest2() throws Exception {
    Assume.assumeTrue(
        ((rsaPublicKey != null && isMariadbServer() && minVersion(12, 1, 1))
            || (!isMariadbServer() && minVersion(8, 0, 0))));
    sharedConnection.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        setConnection(
            "&user=cachingSha256User&password=!Passw0rd3Works&allowPublicKeyRetrieval&serverRsaPublicKeyFile=")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws Exception {
    Assume.assumeTrue(
        !Platform.isWindows()
            && ((rsaPublicKey != null && isMariadbServer() && minVersion(12, 1, 1))
                || (!isMariadbServer() && minVersion(8, 0, 0))));
    sharedConnection.createStatement().execute("FLUSH PRIVILEGES"); // reset cache
    try (Connection con =
        setConnection("&user=cachingSha256User&password=!Passw0rd3Works&allowPublicKeyRetrieval")) {
      con.isValid(1);
    }
  }

  @Test
  public void cachingSha256PluginTestException() throws Exception {
    Assume.assumeTrue(
        (isMariadbServer() && minVersion(12, 1, 1)) || (!isMariadbServer() && minVersion(8, 0, 0)));
    sharedConnection.createStatement().execute("FLUSH PRIVILEGES"); // reset cache

    assertThrows(
        SQLException.class,
        () ->
            setConnection(
                "&user=cachingSha256User&password=!Passw0rd3Works&serverRsaPublicKeyFile=&allowPublicKeyRetrieval=false"));
  }
}
