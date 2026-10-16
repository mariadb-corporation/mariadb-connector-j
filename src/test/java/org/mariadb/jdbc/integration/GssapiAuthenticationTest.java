// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import java.sql.*;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class GssapiAuthenticationTest extends Common {

  @Test
  public void nativePassword() throws Exception {
    Assumptions.assumeTrue(isWindows() && isMariaDBServer());
    // the server announces the Kerberos mechanism by default: the OS user needs a Kerberos
    // credential, which only a domain-joined host has (SSPI otherwise reports "No credentials
    // are available in the security package")
    Assumptions.assumeTrue(
        System.getenv("USERDNSDOMAIN") != null, "Kerberos SSO needs a domain-joined Windows host");
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("INSTALL SONAME 'auth_gssapi'");
    } catch (SQLException e) {
      // eat
    }

    stmt.execute("DROP USER IF EXISTS " + System.getProperty("user.name"));
    stmt.execute("CREATE USER " + System.getProperty("user.name") + " IDENTIFIED VIA gssapi");
    stmt.execute("GRANT ALL PRIVILEGES ON *.* TO " + System.getProperty("user.name"));

    String gssapiUrl =
        String.format(
            "jdbc:mariadb://%s:%s/%s?%s",
            hostname, port, database, defaultOther.replaceFirst("^&", ""));
    try (Connection con = DriverManager.getConnection(gssapiUrl)) {
      con.createStatement().execute("SELECT 1");
    }
  }
}
