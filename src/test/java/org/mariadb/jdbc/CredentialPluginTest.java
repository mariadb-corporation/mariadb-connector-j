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

package org.mariadb.jdbc;

import java.sql.*;
import org.junit.*;

public class CredentialPluginTest extends BaseTest {

  /**
   * Create temporary test User.
   *
   * @throws SQLException if any
   */
  @Before
  public void beforeTest() throws SQLException {
    boolean useOldNotation = true;
    if ((isMariadbServer() && minVersion(10, 2, 0))
        || (!isMariadbServer() && minVersion(8, 0, 0))) {
      useOldNotation = false;
    }
    Statement stmt = sharedConnection.createStatement();
    if (useOldNotation) {
      stmt.execute("CREATE USER 'identityUser'@'localhost'");
      stmt.execute(
          "GRANT SELECT ON "
              + database
              + ".* TO 'identityUser'@'localhost' IDENTIFIED BY '!Passw0rd3Works'");
      stmt.execute("CREATE USER 'identityUser'@'%'");
      stmt.execute(
          "GRANT SELECT ON "
              + database
              + ".* TO 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
    } else {
      stmt.execute("CREATE USER 'identityUser'@'localhost' IDENTIFIED BY '!Passw0rd3Works'");
      stmt.execute("GRANT SELECT ON " + database + ".* TO 'identityUser'@'localhost'");
      stmt.execute("CREATE USER 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
      stmt.execute("GRANT SELECT ON " + database + ".* TO 'identityUser'@'%'");
    }
    stmt.execute("FLUSH PRIVILEGES");
  }

  /**
   * remove temporary test User.
   *
   * @throws SQLException if any
   */
  @After
  public void afterTest() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("DROP USER IF EXISTS 'identityUser'@'%'");
    stmt.execute("DROP USER IF EXISTS 'identityUser'@'localhost'");
  }

  @Test
  public void propertiesIdentityTest() throws SQLException {
    System.setProperty("mariadb.user", "identityUser");
    System.setProperty("mariadb.pwd", "!Passw0rd3Works");
    try (Connection connection = setConnection()) {
      // to ensure not having too many connection error for maxscale
      connection.isValid(1);
    }
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?credentialType=PROPERTY"
                + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
                + "&allowPublicKeyRetrieval")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void specificPropertiesIdentityTest() throws SQLException {

    System.setProperty("myUserKey", "identityUser");
    System.setProperty("myPwdKey", "!Passw0rd3Works");

    try (Connection connection = setConnection()) {
      // to ensure not having too many connection error for maxscale
      connection.isValid(1);
    }
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?credentialType=PROPERTY"
                + "&userKey=myUserKey&pwdKey=myPwdKey"
                + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
                + "&allowPublicKeyRetrieval")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }
}
