// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration;

import java.sql.*;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class GssapiAuthenticationTest extends Common {

  @Test
  public void nativePassword() throws Exception {
    Assumptions.assumeTrue(isWindows());
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("INSTALL SONAME 'auth_gssapi'");
    } catch (SQLException e) {
      // eat
    }
    System.out.println("user name:" + System.getProperty("user.name"));
    stmt.execute("CREATE USER " + System.getProperty("user.name") + " IDENTIFIED VIA gssapi");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO " + System.getProperty("user.name"));

    String gssapiUrl = String.format("jdbc:mariadb://%s:%s/%s", hostname, port, database);
    try (Connection con = DriverManager.getConnection(gssapiUrl)) {
      con.createStatement().execute("SELECT 1");
    }
  }
}
