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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.util.constants.HaMode;

public class FailoverTest extends Common {

  @Test
  public void simpleFailoverTransactionReplay() throws SQLException {
    Assumptions.assumeTrue(System.getenv("SKYSQL") == null && System.getenv("SKYSQL_HA") == null);
    try (Connection con = createProxyCon(HaMode.SEQUENTIAL, "")) {
      con.setNetworkTimeout(Runnable::run, 200);
      long threadId = con.getContext().getThreadId();
      Statement stmt = con.createStatement();
      proxy.restart(200);
      stmt.executeQuery("SELECT 1");
      Assertions.assertTrue(con.getContext().getThreadId() != threadId);
    }
  }

  @Test
  public void transactionReplay() throws SQLException {
    transactionReplay(true);
    transactionReplay(false);
  }

  private void transactionReplay(boolean transactionReplay) throws SQLException {
    Assumptions.assumeTrue(System.getenv("SKYSQL") == null && System.getenv("SKYSQL_HA") == null);
    Statement st = sharedConn.createStatement();
    st.execute("DROP TABLE IF EXISTS transaction_failover");
    st.execute(
        "CREATE TABLE transaction_failover "
            + "(id int not null primary key auto_increment, test varchar(20)) "
            + "engine=innodb");

    try (Connection con =
        createProxyCon(HaMode.SEQUENTIAL, "&transactionReplay=" + transactionReplay)) {
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
      } else {
        assertThrowsContains(
            SQLTransientConnectionException.class,
            () -> stmt.executeUpdate("INSERT INTO transaction_failover (test) VALUES ('test3')"),
            "In progress transaction was lost");
      }
    }
  }

  @Test
  public void transactionReplayPreparedStatement() throws Exception {
    Assumptions.assumeTrue(System.getenv("SKYSQL") == null && System.getenv("SKYSQL_HA") == null);
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
        proxy.restart(300);
        p.setString(1, "test3");
        if (transactionReplay) {
          p.execute();
        } else {
          assertThrowsContains(
              SQLTransientConnectionException.class,
              () -> p.execute(),
              "In progress transaction was lost");
        }
      }
      if (transactionReplay) {
        con.commit();
        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_failover_3");

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
    Assumptions.assumeTrue(System.getenv("SKYSQL") == null && System.getenv("SKYSQL_HA") == null);
    for (int i = 0; i < 8; i++) {
      transactionReplayPreparedStatementBatch((i & 1) > 0, (i & 2) > 0, (i & 4) > 0);
    }
  }

  private void transactionReplayPreparedStatementBatch(
      boolean text, boolean useBulk, boolean transactionReplay) throws SQLException {
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
                + transactionReplay)) {
      stmt = con.createStatement();
      con.setNetworkTimeout(Runnable::run, 200);
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
          assertThrowsContains(
              SQLTransientConnectionException.class,
              () -> p.executeBatch(),
              "In progress transaction was lost");
        }
      }
    }
  }
}
