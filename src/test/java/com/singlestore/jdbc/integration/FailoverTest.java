// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.export.HaMode;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

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
      ;
      Assertions.assertTrue(con.getContext().getThreadId() != threadId);
    }
  }

  @Test
  public void transactionReplay() throws SQLException {
    Assumptions.assumeFalse("8.1.8".equals(sharedConn.getMetaData().getDatabaseProductVersion()));
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
      assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
      final Statement stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 1000);
      long threadId = con.getContext().getThreadId();

      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test0')");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test1')");
      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test2')");
      proxy.restart(1100);
      if (transactionReplay) {
        stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test3')");
        con.commit();

        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover order by id");
        for (int i = 0; i < 4; i++) {
          assertTrue(rs.next());
          assertEquals("test" + i, rs.getString("test"));
        }
        con.commit();
        Assertions.assertTrue(con.getContext().getThreadId() != threadId);
        assertFalse(con.getAutoCommit());
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
      } else {
        Common.assertThrowsContains(
            SQLException.class,
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
    st.execute("DROP TABLE IF EXISTS transaction_failover");
    st.execute(
        "CREATE TABLE transaction_failover "
            + "(id int not null primary key auto_increment, test varchar(20)) "
            + "engine=innodb");

    try (Connection con =
        createProxyCon(HaMode.SEQUENTIAL, "&transactionReplay=" + transactionReplay)) {
      assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
      final Statement stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 2000);
      long threadId = con.getContext().getThreadId();

      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test0')");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test1')");
      stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test2')");
      proxy.restart(1000);
      if (transactionReplay) {
        Common.assertThrowsContains(
            SQLException.class,
            () -> con.commit(),
            "Driver has reconnect connection after a communications failure");

        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover order by id");
        for (int i = 0; i < 1; i++) {
          assertTrue(rs.next());
          assertEquals("test" + i, rs.getString("test"));
        }

        Assertions.assertTrue(con.getContext().getThreadId() != threadId);
        assertFalse(con.getAutoCommit());
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
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
        createRowstore()
            + " TABLE transaction_failover_3 "
            + "(id int not null primary key auto_increment, test varchar(20)) ");

    try (Connection con =
        createProxyCon(
            HaMode.SEQUENTIAL,
            "&useServerPrepStmts=" + binary + "&transactionReplay=" + transactionReplay)) {
      stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 1000);
      long threadId = con.getContext().getThreadId();

      stmt.executeUpdate("INSERT INTO transaction_failover_3 (test) VALUES ('test0')");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO transaction_failover_3 (test) VALUES ('test1')");
      try (PreparedStatement p =
          con.prepareStatement("INSERT INTO transaction_failover_3 (test) VALUES (?)")) {
        p.setString(1, "test2");
        p.execute();

        proxy.restart(1100);
        p.setString(1, "test3");
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
        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover_3 ORDER BY id");

        for (int i = 0; i < 4; i++) {
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
    for (int i = 0; i < 4; i++) {
      transactionReplayPreparedStatementBatch((i & 1) > 0, (i & 2) > 0);
    }
  }

  private void transactionReplayPreparedStatementBatch(boolean text, boolean transactionReplay)
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
            "&useServerPrepStmts=" + !text + "&transactionReplay=" + transactionReplay)) {
      stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 2000);
      long threadId = con.getContext().getThreadId();

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

        proxy.restart(1000);
        p.setString(1, "test5");
        p.addBatch();
        p.setString(1, "test6");
        p.addBatch();

        if (transactionReplay) {
          p.executeBatch();
          con.commit();

          ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover_2 order by id");
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
          } catch (SQLException e) {
            Throwable ee = (e instanceof BatchUpdateException) ? e.getCause() : e;
            assertTrue(ee.getMessage().contains("In progress transaction was lost"));
          }
        }
      }
    }
  }
}
