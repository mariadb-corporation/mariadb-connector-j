// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE USER 'identityUser'@'localhost' IDENTIFIED BY '!Passw0rd3Works'");
    stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'identityUser'@'localhost'");
    stmt.execute("CREATE USER 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
    stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'identityUser'@'%'");
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

    System.setProperty("singlestore.user", "identityUser");
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

    System.setProperty("singlestore.pwd", "!Passw0rd3Works");

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
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("&user=toto&credentialType=ENV&pwdKey=myPwdKey"),
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
  @SetEnvironmentVariable(key = "SINGLESTORE_USER", value = "identityUser")
  public void envsDefaultIdentityAndPwdTest() throws Exception {
    try (Connection conn = createCon("credentialType=ENV&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  @SetEnvironmentVariable(key = "SINGLESTORE_PWD", value = "!Passw0rd3Works")
  @SetEnvironmentVariable(key = "SINGLESTORE_USER", value = "identityUser")
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
  @SetEnvironmentVariable(key = "SINGLESTORE2_USER", value = "identityUser")
  @SetEnvironmentVariable(key = "SINGLESTORE2_PWD", value = "!Passw0rd3Works")
  @SuppressWarnings("unchecked")
  public void envTestsIdentityTest() throws Exception {
    Assumptions.assumeTrue(haveSsl());
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
