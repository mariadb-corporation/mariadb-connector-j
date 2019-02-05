package org.mariadb.jdbc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import org.junit.Test;

public class BasicFailover extends BaseTest {

  @Test
  public void failoverRetry() throws Throwable {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection("jdbc:mariadb:failover//" + ((hostname != null) ? hostname : "localhost")
          + ":" + port + "/" + database + "?user=" + username
          + ((password != null) ? "&password=" + password : "") + "&socketTimeout=1000");
      Statement stmt = null;
      try {
        stmt = connection.createStatement();
        stmt.execute("SELECT SLEEP(10)");
        fail();
      } catch (SQLNonTransientConnectionException e) {
        //normal error : fail to reconnect, since second execution fail too
      }
      if (stmt != null) {
        stmt.close();
      }
      assertTrue(connection.isClosed());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

}