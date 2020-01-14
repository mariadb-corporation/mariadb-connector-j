/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class CredentialPluginTest extends BaseTest {

  /**
   * Create temporary test User.
   *
   * @throws SQLException if any
   */
  @Before
  public void before() throws SQLException {
    boolean useOldNotation = true;
    if ((isMariadbServer() && minVersion(10, 2, 0))
        || (!isMariadbServer() && minVersion(8, 0, 0))) {
      useOldNotation = false;
    }
    Statement stmt = sharedConnection.createStatement();
    if (useOldNotation) {
      stmt.execute("CREATE USER 'identityUser'@'%'");
      stmt.execute(
          "GRANT ALL PRIVILEGES ON *.* TO 'identityUser'@'%' IDENTIFIED BY 'identityUserPwd'");
    } else {
      stmt.execute("CREATE USER 'identityUser'@'%' IDENTIFIED BY 'identityUserPwd'");
      stmt.execute("GRANT ALL PRIVILEGES ON *.* TO 'identityUser'@'%'");
    }
  }

  /**
   * remove temporary test User.
   *
   * @throws SQLException if any
   */
  @After
  public void after() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("DROP USER 'identityUser'@'%'");
  }

  @Test
  public void propertiesIdentityTest() throws SQLException {
    System.setProperty("mariadb.user", "identityUser");
    System.setProperty("mariadb.pwd", "identityUserPwd");

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?credentialType=PROPERTY")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void specificPropertiesIdentityTest() throws SQLException {
    System.setProperty("myUserKey", "identityUser");
    System.setProperty("myPwdKey", "identityUserPwd");

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:mariadb://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?credentialType=PROPERTY"
                + "&userKey=myUserKey&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }
}
