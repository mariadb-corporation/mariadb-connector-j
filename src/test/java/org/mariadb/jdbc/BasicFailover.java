package org.mariadb.jdbc;

import org.junit.*;

import java.sql.*;

import static org.junit.Assert.*;

public class BasicFailover extends BaseTest {

  @Test
  public void failoverRetry() throws Throwable {
    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:mariadb:failover//"
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + "/"
                + database
                + "?user="
                + username
                + ((password != null) ? "&password=" + password : "")
                + "&socketTimeout=1000")) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("SELECT SLEEP(10)");
        fail();
      } catch (SQLNonTransientConnectionException e) {
        // normal error : fail to reconnect, since second execution fail too
      }
      assertTrue(connection.isClosed());
    }
  }
}
