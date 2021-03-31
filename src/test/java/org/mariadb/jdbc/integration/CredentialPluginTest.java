/*
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
 */

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;

public class CredentialPluginTest extends Common {

  /**
   * Create temporary test User.
   *
   * @throws SQLException if any
   */
  @BeforeAll
  public static void beforeTest() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    drop();
    boolean useOldNotation = true;
    if ((isMariaDBServer() && minVersion(10, 2, 0))
        || (!isMariaDBServer() && minVersion(8, 0, 0))) {
      useOldNotation = false;
    }
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
    stmt.execute("DROP USER IF EXISTS 'identityUser'@'%'");
    stmt.execute("DROP USER IF EXISTS 'identityUser'@'localhost'");
  }

  @Test
  public void propertiesIdentityTest() throws SQLException {
    System.setProperty("mariadb.user", "identityUser");
    assertThrowsContains(
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
    assertThrowsContains(
        SQLException.class,
        () -> createCon("credentialType=UNKNOWN"),
        "No identity plugin registered with the type \"UNKNOWN\"");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void envsIdentityTest() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Map<String, String> tmpEnv = new HashMap<>();
    tmpEnv.put("MARIADB_USER", "identityUser");
    setEnv(tmpEnv);

    assertThrowsContains(
        SQLException.class, () -> createCon("credentialType=ENV&pwdKey=myPwdKey"), "Access denied");
    tmpEnv.put("myPwdKey", "!Passw0rd3Works");
    setEnv(tmpEnv);

    try (Connection conn = createCon("credentialType=ENV&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }

    tmpEnv.put("MARIADB_PWD", "!Passw0rd3Works");
    setEnv(tmpEnv);

    try (Connection conn = createCon("credentialType=ENV")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void envsIdentityWithPropertiesTest() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Map<String, String> tmpEnv = new HashMap<>();
    tmpEnv.put("myUserKey", "identityUser");
    tmpEnv.put("myPwdKey", "!Passw0rd3Works");
    setEnv(tmpEnv);

    try (Connection conn = createCon("credentialType=ENV&userKey=myUserKey&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void envTestsIdentityTest() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(isMariaDBServer() && haveSsl());
    Map<String, String> tmpEnv = new HashMap<>();
    tmpEnv.put("MARIADB_USER", "identityUser");
    tmpEnv.put("MARIADB_PWD", "!Passw0rd3Works");
    setEnv(tmpEnv);

    assertThrows(SQLException.class, () -> createCon("credentialType=ENVTEST&sslMode=DISABLE"));
    assertThrows(SQLException.class, () -> createCon("credentialType=ENVTEST"));

    try (Connection conn = createCon("credentialType=ENV&sslMode=TRUST")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  /**
   * Hack to add env variable for unit testing only
   *
   * @param newenv new env variable
   * @throws Exception if any exception occurs
   */
  @SuppressWarnings("unchecked")
  protected static void setEnv(Map<String, String> newenv) throws Exception {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField =
          processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv =
          (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class<?>[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class<?> cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.putAll(newenv);
        }
      }
    }
  }
}
