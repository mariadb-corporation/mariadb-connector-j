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

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class StatementTest extends Common {

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE StatementTest");
    sharedConn.createStatement().execute("DROP TABLE executeGenerated");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS StatementTest");
    stmt.execute("CREATE TABLE StatementTest (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("DROP TABLE IF EXISTS executeGenerated");
    stmt.execute(
        "CREATE TABLE executeGenerated (t1 int not null primary key auto_increment, t2 int)");
  }

  @Test
  public void getConnection() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
    assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
    assertEquals(sharedConn, stmt.getConnection());

    stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
    assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());

    stmt =
        sharedConn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
    assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
    assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());

    // not supported
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
  }

  @Test
  public void execute() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertTrue(stmt.execute("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    ResultSet rs = stmt.getGeneratedKeys();
    Assertions.assertNull(rs.getWarnings());
    assertFalse(rs.next());
    assertNotNull(stmt.getResultSet());
    assertEquals(-1, stmt.getUpdateCount());
    assertFalse(stmt.getMoreResults());
    assertEquals(-1, stmt.getUpdateCount());
    assertFalse(stmt.execute("DO 1"));
    Assertions.assertNull(stmt.getResultSet());
    assertEquals(0, stmt.getUpdateCount());
    assertFalse(stmt.getMoreResults());
    assertEquals(-1, stmt.getUpdateCount());

    assertTrue(stmt.execute("SELECT 1", new int[] {1, 2}));
    rs = stmt.getGeneratedKeys();
    assertFalse(rs.next());

    assertTrue(stmt.execute("SELECT 1", new String[] {"test", "test2"}));
    rs = stmt.getGeneratedKeys();
    assertFalse(rs.next());

    stmt.close();
  }

  @Test
  public void executeGenerated() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertFalse(stmt.execute("INSERT INTO executeGenerated(t2) values (100)"));

    SQLException e = Assertions.assertThrows(SQLException.class, () -> stmt.getGeneratedKeys());
    assertTrue(e.getMessage().contains("Cannot return generated keys"));

    assertFalse(
        stmt.execute(
            "INSERT INTO executeGenerated(t2) values (100)", Statement.RETURN_GENERATED_KEYS));
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
  }

  @Test
  public void executeUpdate() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO StatementTest(t1, t2) values (1, 110), (2, 120)");
    assertEquals(
        2, stmt.executeUpdate("UPDATE StatementTest SET t2 = 130 WHERE t2 > 100 AND t2 < 200"));
    assertEquals(2, stmt.getUpdateCount());
    assertFalse(stmt.getMoreResults());
    assertEquals(-1, stmt.getUpdateCount());

    assertEquals(
        2,
        stmt.executeUpdate(
            "UPDATE StatementTest SET t2 = 150 WHERE t2 > 100 AND t2 < 200", new int[] {1, 2}));
    assertEquals(2, stmt.getUpdateCount());

    assertEquals(
        2,
        stmt.executeUpdate(
            "UPDATE StatementTest SET t2 = 150 WHERE t2 > 100 AND t2 < 200",
            new String[] {"test", "test2"}));
    assertEquals(2, stmt.getUpdateCount());

    try {
      stmt.executeUpdate("SELECT 1");
      Assertions.fail();
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage()
              .contains("the given SQL statement produces an unexpected ResultSet object"));
    }
    assertEquals(0, stmt.executeUpdate("DO 1"));
  }

  @Test
  public void executeLargeUpdate() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO StatementTest(t1, t2) values (10, 210), (12, 220)");
    assertEquals(2, stmt.executeLargeUpdate("UPDATE StatementTest SET t2 = 230 WHERE t2 > 200"));
    assertEquals(2L, stmt.getLargeUpdateCount());
    assertFalse(stmt.getMoreResults());
    assertEquals(-1L, stmt.getLargeUpdateCount());

    assertEquals(
        2,
        stmt.executeLargeUpdate(
            "UPDATE StatementTest SET t2 = 250 WHERE t2 > 200", new int[] {1, 2}));
    assertEquals(2L, stmt.getLargeUpdateCount());

    assertEquals(
        2,
        stmt.executeLargeUpdate(
            "UPDATE StatementTest SET t2 = 250 WHERE t2 > 200", new String[] {"test", "test2"}));
    assertEquals(2L, stmt.getLargeUpdateCount());

    try {
      stmt.executeLargeUpdate("SELECT 1");
      Assertions.fail();
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage()
              .contains("the given SQL statement produces an unexpected ResultSet object"));
    }
    assertEquals(0, stmt.executeLargeUpdate("DO 1"));
  }

  @Test
  public void executeQuery() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1");
    assertTrue(rs.next());

    rs = stmt.executeQuery("DO 1");
    assertFalse(rs.next());
  }

  @Test
  public void close() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    assertFalse(stmt.isClosed());
    ResultSet rs = stmt.executeQuery("select * FROM mysql.user LIMIT 1");
    rs.next();
    Object[] objs = new Object[45];
    for (int i = 0; i < 45; i++) {
      objs[i] = rs.getObject(i + 1);
    }

    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    assertFalse(rs.isClosed());
    stmt.close();
    assertTrue(stmt.isClosed());
    assertTrue(rs.isClosed());

    assertThrowsContains(
        SQLException.class,
        () -> stmt.clearBatch(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.isPoolable(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.setPoolable(true),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.closeOnCompletion(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.isCloseOnCompletion(),
        "Cannot do an operation on a closed statement");

    assertThrowsContains(
        SQLException.class,
        () -> stmt.getResultSetConcurrency(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getFetchSize(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getMoreResults(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.execute("ANY"),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.executeUpdate("ANY"),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.executeQuery("ANY"),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.executeBatch(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getConnection(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getMoreResults(1),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class, () -> stmt.cancel(), "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getMaxRows(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getLargeMaxRows(),
        "Cannot do an operation on a closed statement");

    assertThrowsContains(
        SQLException.class,
        () -> stmt.setMaxRows(1),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.setEscapeProcessing(true),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getQueryTimeout(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getUpdateCount(),
        "Cannot do an operation on a closed statement");
    assertThrowsContains(
        SQLException.class,
        () -> stmt.getLargeUpdateCount(),
        "Cannot do an operation on a closed statement");
  }

  @Test
  public void maxRows() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    assertEquals(0, stmt.getMaxRows());
    try {
      stmt.setMaxRows(-1);
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("max rows cannot be negative"));
    }

    stmt.setMaxRows(10);
    assertEquals(10, stmt.getMaxRows());

    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    int i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);

    stmt.setQueryTimeout(2);
    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);
  }

  @Test
  public void largeMaxRows() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    assertEquals(0L, stmt.getLargeMaxRows());
    try {
      stmt.setLargeMaxRows(-1);
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("max rows cannot be negative"));
    }

    stmt.setLargeMaxRows(10);
    assertEquals(10L, stmt.getLargeMaxRows());

    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    int i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);

    stmt.setQueryTimeout(2);
    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);
  }

  @Test
  public void checkFixedData() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertFalse(stmt.isPoolable());
    stmt.setPoolable(true);
    assertFalse(stmt.isPoolable());
    assertFalse(stmt.isWrapperFor(String.class));
    assertFalse(stmt.isWrapperFor(null));
    assertTrue(stmt.isWrapperFor(Statement.class));
    stmt.unwrap(java.sql.Statement.class);

    assertThrowsContains(
        SQLException.class,
        () -> stmt.unwrap(String.class),
        "he receiver is not a wrapper and does not implement the interface");
    assertThrowsContains(
        SQLException.class, () -> stmt.setCursorName(""), "Cursors are not supported");

    assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
    assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
    assertEquals(0, stmt.getMaxFieldSize());
    stmt.setMaxFieldSize(100);
    assertEquals(0, stmt.getMaxFieldSize());
  }

  @Test
  public void getMoreResults() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    assertFalse(rs.isClosed());

    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    assertTrue(rs.isClosed());
    stmt.close();
  }

  @Test
  @Timeout(20)
  public void queryTimeout() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();

    assertThrowsContains(
        SQLException.class, () -> stmt.setQueryTimeout(-1), "Query timeout cannot be negative");

    assertThrowsContains(
        SQLTimeoutException.class,
        () -> {
          stmt.setQueryTimeout(1);
          assertEquals(1, stmt.getQueryTimeout());
          stmt.execute(
              "select * from information_schema.columns as c1,  information_schema.tables, information_schema"
                  + ".tables as t2");
        },
        "Query execution was interrupted (max_statement_time exceeded)");
  }

  @Test
  public void escaping() throws Exception {
    try (Connection con =
        (Connection) DriverManager.getConnection(mDefUrl + "&dumpQueriesOnException=true")) {
      Statement stmt = con.createStatement();
      assertThrowsContains(
          SQLException.class,
          () ->
              stmt.executeQuery(
                  "select {fn timestampdiff(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df "),
          "select {fn timestampdiff" + "(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df ");
      stmt.setEscapeProcessing(true);
      assertThrowsContains(
          SQLException.class,
          () ->
              stmt.executeQuery(
                  "select {fn timestampdiff(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df "),
          "select timestampdiff(HOUR, '2003-02-01','2003-05-01') df df ");
    }
  }

  @Test
  public void testWarnings() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();

    // connection level
    Assertions.assertNull(sharedConn.getWarnings());
    stmt.executeQuery("select now() = 1");
    SQLWarning warning = sharedConn.getWarnings();
    assertTrue(warning.getMessage().contains("ncorrect datetime value: '1'"));
    stmt.executeQuery("select now() = 1");
    sharedConn.clearWarnings();
    Assertions.assertNull(sharedConn.getWarnings());

    // statement level
    ResultSet rs = stmt.executeQuery("select now() = 1");
    warning = rs.getWarnings();
    assertTrue(warning.getMessage().contains("ncorrect datetime value: '1'"));

    rs = stmt.executeQuery("select now() = 1");
    rs.clearWarnings();
    Assertions.assertNull(rs.getWarnings());

    stmt.executeQuery("select now() = 1");
    warning = stmt.getWarnings();
    assertTrue(warning.getMessage().contains("ncorrect datetime value: '1'"));

    stmt.executeQuery("select now() = 1");
    stmt.clearWarnings();
    Assertions.assertNull(stmt.getWarnings());
  }

  @Test
  public void cancel() throws Exception {
    Assumptions.assumeTrue(
        isMariaDBServer()
            && System.getenv("MAXSCALE_TEST_DISABLE") == null
            && System.getenv("SKYSQL_HA") == null);
    Statement stmt = sharedConn.createStatement();
    stmt.cancel(); // will do nothing

    ExecutorService exec = Executors.newFixedThreadPool(1);

    assertThrowsContains(
        SQLTimeoutException.class,
        () -> {
          exec.execute(new CancelThread(stmt));
          stmt.execute(
              "select * from information_schema.columns as c1,  information_schema.tables, information_schema"
                  + ".tables as t2");
          exec.shutdown();
        },
        "Query execution was interrupted");
  }

  @Test
  public void fetch() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    assertThrowsContains(SQLException.class, () -> stmt.setFetchSize(-10), "invalid fetch size");

    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_10000");

    for (int i = 1; i <= 10000; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    assertFalse(rs.next());
  }

  @Test
  public void fetchUnFinishedSameStatement() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_1000");

    for (int i = 1; i <= 500; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    ResultSet rs2 = stmt.executeQuery("select * FROM seq_1_to_1000");

    for (int i = 501; i <= 1000; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    for (int i = 1; i <= 1000; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void fetchUnFinishedOtherStatement() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(5);
    assertEquals(5, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_20");

    for (int i = 1; i <= 10; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM seq_1_to_20");

    for (int i = 11; i <= 20; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    for (int i = 1; i <= 20; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void fetchClose() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_1000");

    for (int i = 1; i <= 500; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    stmt.close();
    assertTrue(rs.isClosed());

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM seq_1_to_1000");
    for (int i = 1; i <= 1000; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void executeBatchBasic() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS executeBatchBasic");
    stmt.execute(
        "CREATE TABLE executeBatchBasic (t1 int not null primary key auto_increment, t2 int)");

    assertThrowsContains(
        SQLException.class,
        () -> stmt.addBatch(null),
        "null cannot be set to addBatch(String sql)");

    stmt.addBatch("INSERT INTO executeBatchBasic(t2) VALUES (55)");
    stmt.setEscapeProcessing(true);
    stmt.addBatch("INSERT INTO executeBatchBasic(t2) VALUES (56)");
    int[] ret = stmt.executeBatch();
    Assertions.assertArrayEquals(new int[] {1, 1}, ret);

    ret = stmt.executeBatch();
    Assertions.assertArrayEquals(new int[0], ret);

    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (57)");
    stmt.clearBatch();
    ret = stmt.executeBatch();
    Assertions.assertArrayEquals(new int[0], ret);

    stmt.addBatch("WRONG QUERY");
    assertThrowsContains(
        BatchUpdateException.class,
        () -> stmt.executeBatch(),
        "You have an error in your SQL syntax");
  }

  @Test
  public void executeLargeBatchBasic() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS executeLargeBatchBasic");
    stmt.execute(
        "CREATE TABLE executeLargeBatchBasic (t1 int not null primary key auto_increment, t2 int)");
    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (55)");
    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (56)");
    long[] ret = stmt.executeLargeBatch();
    Assertions.assertArrayEquals(new long[] {1, 1}, ret);

    ret = stmt.executeLargeBatch();
    Assertions.assertArrayEquals(new long[0], ret);

    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (57)");
    stmt.clearBatch();
    ret = stmt.executeLargeBatch();
    Assertions.assertArrayEquals(new long[0], ret);

    stmt.addBatch("WRONG QUERY");
    assertThrowsContains(
        BatchUpdateException.class,
        () -> stmt.executeLargeBatch(),
        "You have an error in your SQL syntax");
  }

  @Test
  public void fetchSize() throws SQLException {
    assertEquals(0, sharedConn.createStatement().getFetchSize());
    try (Connection con = createCon("&defaultFetchSize=10")) {
      assertEquals(10, con.createStatement().getFetchSize());
      try (PreparedStatement prep = con.prepareStatement("SELECT ?")) {
        assertEquals(10, prep.getFetchSize());
      }
    }
  }

  @Test
  public void moreResults() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multi");
    stmt.setFetchSize(3);
    stmt.execute(
        "CREATE PROCEDURE multi() BEGIN SELECT * from seq_1_to_10; SELECT * FROM seq_1_to_1000;SELECT 2; END");
    stmt.execute("CALL multi()");
    assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      assertEquals(i++, rs.getInt(1));
    }
    assertEquals(1001, i);
    stmt.setFetchSize(3);

    rs = stmt.executeQuery("CALL multi()");
    assertFalse(rs.isClosed());
    stmt.setFetchSize(0); // force more result to load all remaining result-set
    assertTrue(stmt.getMoreResults());
    assertTrue(rs.isClosed());
    rs = stmt.getResultSet();
    i = 1;
    while (rs.next()) {
      assertEquals(i++, rs.getInt(1));
    }

    stmt.setFetchSize(3);
    rs = stmt.executeQuery("CALL multi()");
    assertFalse(rs.isClosed());
    stmt.setFetchSize(0); // force more result to load all remaining result-set
    assertTrue(stmt.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
    assertFalse(rs.isClosed());
    i = 1;
    while (rs.next()) {
      assertEquals(i++, rs.getInt(1));
    }
    assertEquals(11, i);
    rs = stmt.getResultSet();
    i = 1;
    while (rs.next()) {
      assertEquals(i++, rs.getInt(1));
    }
    assertEquals(1001, i);

    rs = stmt.executeQuery("CALL multi()");
    stmt.close();
    assertTrue(rs.isClosed());
  }

  @Test
  public void closeOnCompletion() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertFalse(stmt.isCloseOnCompletion());
    stmt.closeOnCompletion();
    assertTrue(stmt.isCloseOnCompletion());
    assertFalse(stmt.isClosed());
    ResultSet rs = stmt.executeQuery("SELECT 1");
    assertFalse(rs.isClosed());
    assertFalse(stmt.isClosed());
    rs.close();
    assertTrue(rs.isClosed());
    assertTrue(stmt.isClosed());
  }

  private static class CancelThread implements Runnable {

    private final java.sql.Statement stmt;

    public CancelThread(java.sql.Statement stmt) {
      this.stmt = stmt;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(100);
        stmt.cancel();
      } catch (SQLException | InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
