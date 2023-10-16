// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class CredentialPluginTest extends Common {

  /**
   * Create temporary test User.
   *
   * @throws SQLException if any
   */
  @BeforeAll
  public static void beforeTest() throws SQLException {
    String version = System.getProperty("java.version");
    int majorVersion = Integer.parseInt(version.substring(0, version.indexOf(".")));
    Assumptions.assumeTrue(majorVersion < 17);

    Assumptions.assumeTrue(isMariaDBServer());
    drop();
    boolean useOldNotation =
        (!isMariaDBServer() || !minVersion(10, 2, 0))
            && (isMariaDBServer() || !minVersion(8, 0, 0));
    Statement stmt = sharedConn.createStatement();
    if (useOldNotation) {
      stmt.execute("CREATE USER 'identityUser'@'localhost'");
      stmt.execute(
          "GRANT SELECT ON "
              + sharedConn.getCatalog()
              + ".* TO 'identityUser'@'localhost' IDENTIFIED BY '!Passw0rd3Works'");
      stmt.execute("CREATE USER 'identityUser'@'%'");
      stmt.execute(
          "GRANT SELECT ON "
              + sharedConn.getCatalog()
              + ".* TO 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
    } else {
      stmt.execute("CREATE USER 'identityUser'@'localhost' IDENTIFIED BY '!Passw0rd3Works'");
      stmt.execute(
          "GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'identityUser'@'localhost'");
      stmt.execute("CREATE USER 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
      stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'identityUser'@'%'");
    }
    stmt.execute("FLUSH PRIVILEGES");
  }

  /**
   * remove temporary test User.
   *
   * @throws SQLException if any
   */
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("DROP USER 'identityUser'@'%'");
    } catch (SQLException e) {
      // eat
    }
    try {
      stmt.execute("DROP USER 'identityUser'@'localhost'");
    } catch (SQLException e) {
      // eat
    }
  }

  @Test
  public void propertiesIdentityTest() throws SQLException {
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("credentialType=PROPERTY&user=identityUser"),
        "Access denied");

    System.setProperty("mariadb.user", "identityUser");
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("credentialType=PROPERTY&pwdKey=myPwdKey"),
        "Access denied");

    System.setProperty("myPwdKey", "!Passw0rd3Works");
    try (Connection conn = createCon("credentialType=PROPERTY&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }

    System.setProperty("mariadb.pwd", "!Passw0rd3Works");

    try (Connection conn = createCon("credentialType=PROPERTY")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void specificPropertiesIdentityTest() throws SQLException {

    System.setProperty("myUserKey", "identityUser");
    System.setProperty("myPwdKey", "!Passw0rd3Works");

    try (Connection conn = createCon("credentialType=PROPERTY&userKey=myUserKey&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void unknownCredentialTest() {
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("credentialType=UNKNOWN"),
        "No identity plugin registered with the type \"UNKNOWN\"");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void noEnvsIdentityTest() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("&user=toti&credentialType=ENV&pwdKey=myPwdKey"),
        "Access denied");
  }

  @Test
  @SetEnvironmentVariable(key = "myPwdKey", value = "!Passw0rd3Works")
  public void envsPwdTest() throws Exception {
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("&user=toto&credentialType=ENV&pwdKey=myPwdKey"),
        "Access denied");

    try (Connection conn = createCon("user=identityUser&credentialType=ENV&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  @SetEnvironmentVariable(key = "myPwdKey", value = "!Passw0rd3Works")
  @SetEnvironmentVariable(key = "MARIADB_USER", value = "identityUser")
  public void envsDefaultIdentityAndPwdTest() throws Exception {
    try (Connection conn = createCon("credentialType=ENV&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  @SetEnvironmentVariable(key = "MARIADB_PWD", value = "!Passw0rd3Works")
  @SetEnvironmentVariable(key = "MARIADB_USER", value = "identityUser")
  public void envsIdentityDefaultPwdTest() throws Exception {

    try (Connection conn = createCon("credentialType=ENV")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  @SetEnvironmentVariable(key = "myPwdKey", value = "!Passw0rd3Works")
  @SetEnvironmentVariable(key = "myUserKey", value = "identityUser")
  public void envsIdentityAndPwdTest() throws Exception {

    try (Connection conn = createCon("credentialType=ENV&userKey=myUserKey&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  // @ClearSystemProperty(key = "some key")
  @SetEnvironmentVariable(key = "MARIADB2_USER", value = "identityUser")
  @SetEnvironmentVariable(key = "MARIADB2_PWD", value = "!Passw0rd3Works")
  @SuppressWarnings("unchecked")
  public void envTestsIdentityTest() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(isMariaDBServer() && haveSsl());

    assertThrows(SQLException.class, () -> createCon("credentialType=ENVTEST&sslMode=DISABLE"));
    assertThrows(SQLException.class, () -> createCon("credentialType=ENVTEST"));

    try (Connection conn = createCon("credentialType=ENVTEST&sslMode=TRUST")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }
}
