// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.sql.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.export.HaMode;

public class FailoverTest extends Common {

  @Test
  public void simpleFailoverTransactionReplay() throws SQLException {
    try (Connection con = createProxyCon(HaMode.SEQUENTIAL, "")) {
      con.setNetworkTimeout(Runnable::run, 200);
      long threadId = con.getContext().getThreadId();
      Statement stmt = con.createStatement();
      proxy.restart(200);
      assertThrowsContains(
          SQLTransientConnectionException.class,
          () -> stmt.execute("SELECT 1"),
          "Driver has reconnect connection after a communications link failure");
      Assertions.assertTrue(con.getContext().getThreadId() != threadId);
    }
  }

  @Test
  public void simpleFailoverTransactionReplayNoQuery() throws SQLException {
    try (Connection con = createProxyCon(HaMode.SEQUENTIAL, "")) {
      con.setNetworkTimeout(Runnable::run, 200);
      long threadId = con.getContext().getThreadId();
      Statement stmt = con.createStatement();
      proxy.restart(200);

      con.isValid(1000);
      Assertions.assertTrue(con.getContext().getThreadId() != threadId);
    }
  }

  @Test
  public void transactionReplay() throws SQLException {
    transactionReplay(true);
    transactionReplay(false);
  }

  private void transactionReplay(boolean transactionReplay) throws SQLException {
    Statement st = sharedConn.createStatement();
    st.execute("DROP TABLE IF EXISTS transaction_failover");
    st.execute(
        "CREATE TABLE transaction_failover "
            + "(id int not null primary key auto_increment, test varchar(20)) "
            + "engine=innodb");

    try (Connection con =
        createProxyCon(HaMode.SEQUENTIAL, "&transactionReplay=" + transactionReplay)) {
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation());
      con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      final Statement stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 200);
      long threadId = con.getContext().getThreadId();

      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test0')");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test1')");
      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test2')");
      proxy.restart(300);
      if (transactionReplay) {
        stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test3')");
        con.commit();

        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover");
        for (int i = 0; i < 4; i++) {
          assertTrue(rs.next());
          assertEquals("test" + i, rs.getString("test"));
        }
        con.commit();
        Assertions.assertTrue(con.getContext().getThreadId() != threadId);
        assertFalse(con.getAutoCommit());
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation());
      } else {
        Common.assertThrowsContains(
            SQLTransientConnectionException.class,
            () -> stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test3')"),
            "In progress transaction was lost");
      }
    }
  }

  @Test
  public void transactionReplayDuringCommit() throws SQLException {
    transactionReplayDuringCommit(true);
    transactionReplayDuringCommit(false);
  }

  private void transactionReplayDuringCommit(boolean transactionReplay) throws SQLException {
    Statement st = sharedConn.createStatement();
    String tableName = "transaction_failover_" + (transactionReplay ? "1" : "2");
    st.execute("DROP TABLE IF EXISTS " + tableName);
    st.execute(
        "CREATE TABLE " + tableName
            + " (id int not null primary key auto_increment, test varchar(20)) "
            + "engine=innodb");

    try (Connection con =
        createProxyCon(HaMode.SEQUENTIAL, "&transactionReplay=" + transactionReplay)) {
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation());
      con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      final Statement stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 200);
      long threadId = con.getContext().getThreadId();

      stmt.executeUpdate("INSERT INTO " + tableName + " (test) VALUES ('test0')");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO " + tableName + " (test) VALUES ('test1')");
      stmt.executeUpdate("INSERT INTO " + tableName + " (test) VALUES ('test2')");
      proxy.restart(300);
      if (transactionReplay) {
        Common.assertThrowsContains(
            SQLTransientConnectionException.class,
            con::commit,
            "Driver has reconnect connection after a communications failure");

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        for (int i = 0; i < 1; i++) {
          assertTrue(rs.next());
          assertEquals("test" + i, rs.getString("test"));
        }

        Assertions.assertTrue(con.getContext().getThreadId() != threadId);
        assertFalse(con.getAutoCommit());
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation());
      } else {
        Common.assertThrowsContains(
            SQLTransientConnectionException.class, con::commit, "during a COMMIT statement");
      }
    }
  }

  @Test
  public void transactionReplayPreparedStatement() throws Exception {
    transactionReplayPreparedStatement(true, true);
    transactionReplayPreparedStatement(false, true);
    transactionReplayPreparedStatement(true, false);
    transactionReplayPreparedStatement(false, false);
  }

  private void transactionReplayPreparedStatement(boolean binary, boolean transactionReplay)
      throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS transaction_failover_3");
    stmt.execute(
        "CREATE TABLE transaction_failover_3 "
            + "(id int not null primary key auto_increment, test varchar(20)) "
            + "engine=innodb");

    try (Connection con =
        createProxyCon(
            HaMode.SEQUENTIAL,
            "&useServerPrepStmts=" + binary + "&transactionReplay=" + transactionReplay)) {
      stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 200);
      long threadId = con.getContext().getThreadId();

      stmt.executeUpdate("INSERT INTO transaction_failover_3 (test) VALUES ('test0')");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO transaction_failover_3 (test) VALUES ('test1')");
      try (PreparedStatement p =
          con.prepareStatement("INSERT INTO transaction_failover_3 (test) VALUES (?)")) {
        p.setString(1, "test2");
        p.execute();
        p.setAsciiStream(1, new ByteArrayInputStream("test3".getBytes()));
        p.execute();

        proxy.restart(300);
        p.setString(1, "test4");
        if (transactionReplay) {
          p.execute();
        } else {
          Common.assertThrowsContains(
              SQLTransientConnectionException.class,
              p::execute,
              "In progress transaction was lost");
        }
      }
      if (transactionReplay) {
        con.commit();
        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover_3");

        for (int i = 0; i < 5; i++) {
          assertTrue(rs.next());
          assertEquals("test" + i, rs.getString("test"));
        }
        con.commit();
        Assertions.assertTrue(con.getContext().getThreadId() != threadId);
        assertFalse(con.getAutoCommit());
      }
    }
  }

  @Test
  public void transactionReplayPreparedStatementBatch() throws Exception {
    Assumptions.assumeTrue(!isMaxscale());
    for (int i = 0; i < 16; i++) {
      System.out.println("transactionReplayPreparedStatementBatch:" + i);
      transactionReplayPreparedStatementBatch((i & 1) > 0, (i & 2) > 0, (i & 4) > 0, (i & 8) > 0);
    }
  }

  private void transactionReplayPreparedStatementBatch(
      boolean text, boolean useBulk, boolean transactionReplay, boolean useRewrite)
      throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS transaction_failover_2");
    stmt.execute(
        "CREATE TABLE transaction_failover_2 "
            + "(id int not null primary key auto_increment, test varchar(20)) "
            + "engine=innodb");

    try (Connection con =
        createProxyCon(
            HaMode.SEQUENTIAL,
            "&useServerPrepStmts="
                + !text
                + "&useBulkStmts="
                + useBulk
                + "&transactionReplay="
                + transactionReplay
                + "&rewriteBatchedStatements="
                + useRewrite)) {
      con.setNetworkTimeout(Runnable::run, 500);
      long threadId = con.getContext().getThreadId();
      execute(con, transactionReplay, threadId);
      threadId = con.getContext().getThreadId();
      execute(con, transactionReplay, threadId);
    }
  }

  private void execute(Connection con, boolean transactionReplay, long threadId)
      throws SQLException {
    Statement stmt = con.createStatement();

    stmt.executeUpdate("INSERT INTO transaction_failover_2 (test) VALUES ('test0')");
    con.setAutoCommit(false);
    stmt.executeUpdate("INSERT INTO transaction_failover_2 (test) VALUES ('test1')");
    try (PreparedStatement p =
        con.prepareStatement("INSERT INTO transaction_failover_2 (test) VALUES (?)")) {
      p.setString(1, "test2");
      p.execute();
      p.setString(1, "test3");
      p.addBatch();
      p.setString(1, "test4");
      p.addBatch();
      p.executeBatch();

      proxy.restart(300);
      p.setString(1, "test5");
      p.addBatch();
      p.setString(1, "test6");
      p.addBatch();

      if (transactionReplay) {
        p.executeBatch();
        con.commit();

        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover_2");
        for (int i = 0; i < 6; i++) {
          assertTrue(rs.next());
          assertEquals("test" + i, rs.getString("test"));
        }
        con.commit();
        Assertions.assertTrue(con.getContext().getThreadId() != threadId);
        assertFalse(con.getAutoCommit());
      } else {
        try {
          p.executeBatch();
          Assertions.fail();
        } catch (BatchUpdateException e) {
          assertEquals(e.getCause().getMessage(), e.getMessage());
          assertEquals(((SQLException) e.getCause()).getSQLState(), e.getSQLState());
          assertEquals(((SQLException) e.getCause()).getErrorCode(), e.getErrorCode());
          assertTrue(e.getCause().getMessage().contains("In progress transaction was lost"));
        } catch (SQLException ee) {
          assertTrue(ee.getMessage().contains("In progress transaction was lost"));
        }
      }
    }
    stmt.execute("TRUNCATE transaction_failover_2");
    stmt.executeUpdate("INSERT INTO transaction_failover_2 (test) VALUES ('test0')");
    con.setAutoCommit(false);
    stmt.executeUpdate("INSERT INTO transaction_failover_2 (test) VALUES ('test1')");
    try (PreparedStatement p =
        con.prepareStatement("INSERT INTO transaction_failover_2 (test)  VALUES (?)")) {

      proxy.restart(300);
      p.setString(1, "test2");
      p.addBatch();
      p.setString(1, "test3");
      p.addBatch();
      p.setString(1, "test4");
      p.addBatch();
      p.setString(1, "test5");
      p.addBatch();

      if (transactionReplay) {
        p.executeBatch();
        con.commit();

        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover_2");
        for (int i = 0; i < 5; i++) {
          assertTrue(rs.next());
          assertEquals("test" + i, rs.getString("test"));
        }
        con.commit();
        Assertions.assertTrue(con.getContext().getThreadId() != threadId);
        assertFalse(con.getAutoCommit());
      } else {
        try {
          p.executeBatch();
          Assertions.fail();
        } catch (BatchUpdateException be) {
          assertTrue(be.getCause().getMessage().contains("In progress transaction was lost"));
        } catch (SQLException e) {
          assertTrue(e.getMessage().contains("In progress transaction was lost"));
        }
      }
    }
  }
}
