// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.integration.tools.TcpProxy;

public class RedirectionTest extends Common {
  @Test
  void basicRedirection() throws Exception {

    Connection connection = createProxyCon(HaMode.NONE, "&permitRedirect=true");
    Assertions.assertEquals("localhost:" + proxy.getLocalPort(), connection.__test_host());
    boolean permitRedirection = true;
    Statement stmt = connection.createStatement();
    try {
      stmt.execute(String.format("set @@session.redirect_url=\"mariadb://%s:%s\"", hostname, port));
    } catch (SQLException e) {
      // if server doesn't support redirection
      permitRedirection = false;
    }
    ResultSet rs = stmt.executeQuery("SELECT 1");
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(1, rs.getInt(1));

    if (permitRedirection) {
      Assertions.assertEquals(
          port == 3306 ? hostname : hostname + ":" + port, connection.__test_host());
    }
    connection.close();
    proxy.stop();
  }

  @Test
  void redirectionDuringTransaction() throws Exception {

    Connection connection = createProxyCon(HaMode.NONE, "&permitRedirect=true");
    Assertions.assertEquals("localhost:" + proxy.getLocalPort(), connection.__test_host());
    boolean permitRedirection = true;
    Statement stmt = connection.createStatement();

    stmt.execute("BEGIN");
    try {
      stmt.execute(String.format("set @@session.redirect_url=\"mariadb://%s:%s\"", hostname, port));
    } catch (SQLException e) {
      // if server doesn't support redirection
      permitRedirection = false;
    }
    ResultSet rs = stmt.executeQuery("SELECT 1");
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(1, rs.getInt(1));
    Assertions.assertEquals("localhost:" + proxy.getLocalPort(), connection.__test_host());
    connection.commit();
    if (permitRedirection) {
      Assertions.assertEquals(
          port == 3306 ? hostname : hostname + ":" + port, connection.__test_host());
    }
    rs = stmt.executeQuery("SELECT 1");
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(1, rs.getInt(1));
    connection.close();
    proxy.stop();
  }

  @Test
  void connectionRedirection() throws Exception {
    // need maxscale 23.08+
    Assumptions.assumeTrue(getMaxScaleVersion() >= 230800);
    try {
      proxy = new TcpProxy(hostname, port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }
    try {
      sharedConn
          .createStatement()
          .execute(
              String.format(
                  "set @@global.redirect_url=\"mariadb://localhost:%s\"", proxy.getLocalPort()));
      Assertions.assertEquals(
          String.format("address=(host=localhost)(port=%s)(type=primary)", proxy.getLocalPort()),
          sharedConn.__test_host());

      try (Connection conn = createCon("&permitRedirect=true")) {
        Assertions.assertEquals(
            String.format("address=(host=localhost)(port=%s)(type=primary)", proxy.getLocalPort()),
            conn.__test_host());
      } finally {
        proxy.stop();
        sharedConn.createStatement().execute("set @@global.redirect_url=\"\"");
      }
    } catch (Exception e) {
      proxy.stop();
    }
  }
}
