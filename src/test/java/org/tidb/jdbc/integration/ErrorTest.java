// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ErrorTest extends Common {

  @BeforeAll
  public static void begin() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE deadlock(a int primary key)");
    stmt.execute("CREATE TABLE deadlock2(a int primary key) ENGINE=InnoDB");

    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS deadlock");
    stmt.execute("DROP TABLE IF EXISTS deadlock2");
  }

  @Test
  public void dumpQueryOnException() throws Exception {
    try (Connection con = createCon("dumpQueriesOnException")) {
      Statement stmt = con.createStatement();
      try {
        stmt.execute("SELECT 'long value' FROM wrongTable");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Query is: SELECT 'long value' FROM wrongTable"));
      }
    }

    try (Connection con = createCon("maxQuerySizeToLog=100&dumpQueriesOnException")) {
      Statement stmt = con.createStatement();
      try {
        stmt.execute("SELECT 'long value' FROM wrongTable");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Query is: SELECT 'long value' FROM wrongTable"));
      }
    }

    try (Connection con = createCon("maxQuerySizeToLog=13&dumpQueriesOnException")) {
      Statement stmt = con.createStatement();
      try {
        stmt.execute("SELECT 'long value' FROM wrongTable");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Query is: SELECT 'lo..."));
      }
    }
  }

  // TiDB not restrict connection number, remove testPre41ErrorFormat

  private void deadlockRunner(
      CountDownLatch waitDone,
      CyclicBarrier waitForSelect,
      boolean lockZeroFirst,
      AtomicInteger successNum) {
    final String select0 = "SELECT * FROM deadlock WHERE a=0 FOR UPDATE";
    final String select1 = "SELECT * FROM deadlock WHERE a=1 FOR UPDATE";

    Connection conn = null;

    try {
      conn = createCon();
      conn.setAutoCommit(false);
      conn.createStatement().execute("START TRANSACTION");
      conn.createStatement().execute(lockZeroFirst ? select0 : select1);
      waitForSelect.await(5, TimeUnit.SECONDS);
      conn.createStatement().execute(lockZeroFirst ? select1 : select0);
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage()
              .contains("Deadlock found when trying to get lock; try restarting transaction"));
      successNum.getAndIncrement();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // eat
        }
      }

      waitDone.countDown();
    }
  }

  @Test
  public void deadLockInformation() throws SQLException, InterruptedException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("insert into deadlock(a) values(0), (1)");

    AtomicInteger successNum = new AtomicInteger(0);
    CyclicBarrier waitForSelect = new CyclicBarrier(2);
    CountDownLatch waitDone = new CountDownLatch(2);
    ExecutorService twoThreadPool = Executors.newFixedThreadPool(2);
    twoThreadPool.execute(() -> deadlockRunner(waitDone, waitForSelect, true, successNum));
    twoThreadPool.execute(() -> deadlockRunner(waitDone, waitForSelect, false, successNum));
    waitDone.await(5, TimeUnit.SECONDS);

    assertNotEquals(successNum.get(), 0);
  }
}
