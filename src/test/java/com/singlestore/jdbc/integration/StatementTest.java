// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.ClientPreparedStatement;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.Statement;
import java.sql.BatchUpdateException;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class StatementTest extends Common {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS StatementTest");
    stmt.execute("DROP TABLE IF EXISTS executeGenerated");
    stmt.execute("DROP TABLE IF EXISTS executeGenerated2");
    stmt.execute("DROP TABLE IF EXISTS testAffectedRow");
    stmt.execute("DROP TABLE IF EXISTS bigIntId");
    stmt.execute("DROP TABLE IF EXISTS testCONJ956");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE testCONJ956 (field varchar(300) NOT NULL)");
    stmt.execute("CREATE TABLE StatementTest (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute(
        "CREATE TABLE executeGenerated (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute(
        "CREATE TABLE executeGenerated2 (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("CREATE TABLE testAffectedRow(id int)");
    stmt.execute(
        "CREATE TABLE bigIntId(`id` bigint(20) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT, val"
            + " VARCHAR(256))");
    createSequenceTables();
    stmt.execute("FLUSH TABLES");
  }

  @Test
  public void ensureGetGeneratedKeysReturnsEmptyResult() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS key_test (id INT(11) NOT NULL)");
    try (PreparedStatement ps =
        sharedConn.prepareStatement(
            "INSERT INTO key_test(id) VALUES(5)", Statement.RETURN_GENERATED_KEYS)) {
      ps.execute();
      ResultSet rs = ps.getGeneratedKeys();
      assertFalse(rs.next());
    }
    try (PreparedStatement ps =
        sharedConn.prepareStatement(
            "UPDATE key_test set id=7 WHERE id=5", Statement.RETURN_GENERATED_KEYS)) {
      ps.execute();
      ResultSet rs = ps.getGeneratedKeys();
      assertFalse(rs.next());
    }

    stmt.execute("DROP TABLE key_test");
  }

  @Test
  public void unsignedMetadataResult() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS unsignedMetadataResult");
    stmt.execute(
        "CREATE TABLE unsignedMetadataResult("
            + "c1 SMALLINT UNSIGNED, "
            + "c2 MEDIUMINT UNSIGNED, "
            + "c3 INTEGER UNSIGNED, "
            + "c4 BIGINT UNSIGNED, "
            + "c5 DOUBLE UNSIGNED, "
            + "c6 FLOAT UNSIGNED, "
            + "c7 DECIMAL UNSIGNED)");
    stmt.execute("INSERT INTO unsignedMetadataResult VALUES(11,12,13,14,15,16,17)");
    assertTrue(stmt.execute("SELECT * FROM unsignedMetadataResult"));

    ResultSet rs = stmt.getResultSet();
    ResultSetMetaData rsMetaData = rs.getMetaData();
    for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
      assertTrue(rsMetaData.getColumnTypeName(i).contains("UNSIGNED"));
    }
    stmt.execute("DROP TABLE unsignedMetadataResult");
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
  public void setObjectError() throws SQLException {
    try (PreparedStatement prep = sharedConn.prepareStatement("SELECT ?")) {
      assertThrowsContains(
          SQLException.class, () -> prep.setObject(1, "", Types.ARRAY), "Type not supported");
      assertThrowsContains(
          SQLException.class, () -> prep.setObject(1, "", JDBCType.ARRAY), "Type not supported");
      assertThrowsContains(
          SQLException.class,
          () -> prep.setObject(1, "a", JDBCType.BLOB),
          "Cannot convert a string to a Blob");
      assertThrowsContains(
          SQLException.class,
          () -> prep.setObject(1, 'a', JDBCType.BLOB),
          "Cannot convert a character to a Blob");
    }
  }

  @Test
  public void conj956() throws SQLException {
    StringBuilder sb = new StringBuilder();
    String sQuery = "SELECT EXISTS (SELECT 1 FROM testCONJ956 WHERE ((field=?)))";
    for (int i = 1; i <= 300; i++) {
      sb.append("a");
      if (i < 204) {
        continue;
      }
      PreparedStatement stmt = sharedConn.prepareStatement(sQuery);
      stmt.setString(1, sb.toString());
      stmt.executeQuery();
    }
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
    assertFalse(stmt.execute("CALL dummy_proc()"));
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
    stmt.execute("DROP TABLE IF EXISTS executeGenerated");
    stmt.execute(
        "CREATE TABLE executeGenerated (t1 int not null primary key auto_increment, t2 int)");
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
  public void executeGeneratedBatch() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.addBatch("INSERT INTO executeGenerated2(t2) values (110)");
    stmt.addBatch("INSERT INTO executeGenerated2(t2) values (120)");
    int[] res = stmt.executeBatch();
    assertArrayEquals(new int[] {1, 1}, res);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertFalse(rs.next());
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
    assertEquals(0, stmt.executeUpdate("CALL dummy_proc()"));
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
    assertEquals(0, stmt.executeLargeUpdate("CALL dummy_proc()"));
  }

  @Test
  public void executeQuery() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1");
    assertTrue(rs.next());

    rs = stmt.executeQuery("CALL dummy_proc()");
    assertFalse(rs.next());
  }

  @Test
  public void close() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertFalse(stmt.isClosed());
    ResultSet rs = stmt.executeQuery("select * FROM information_schema.users LIMIT 1");
    rs.next();
    for (int i = 0; i < 13; i++) {
      rs.getObject(i + 1);
    }

    ensureRange(stmt);
    rs = stmt.executeQuery("SELECT * FROM range_1_100");
    assertFalse(rs.isClosed());
    stmt.close();
    assertTrue(stmt.isClosed());
    assertTrue(rs.isClosed());

    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.clearBatch(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.isPoolable(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.setPoolable(true),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.closeOnCompletion(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.isCloseOnCompletion(),
        "Cannot do an operation on a closed statement");

    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getResultSetConcurrency(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getFetchSize(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getMoreResults(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.execute("ANY"),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.executeUpdate("ANY"),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.executeQuery("ANY"),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.executeBatch(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getConnection(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getMoreResults(1),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, () -> stmt.cancel(), "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getMaxRows(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getLargeMaxRows(),
        "Cannot do an operation on a closed statement");

    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.setMaxRows(1),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.setEscapeProcessing(true),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getQueryTimeout(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getUpdateCount(),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getLargeUpdateCount(),
        "Cannot do an operation on a closed statement");
  }

  @Test
  public void maxRows() throws SQLException {
    Statement stmt = createCon().createStatement();
    assertEquals(0, stmt.getMaxRows());
    try {
      stmt.setMaxRows(-1);
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("max rows cannot be negative"));
    }

    stmt.setMaxRows(10);
    assertEquals(10, stmt.getMaxRows());

    ensureRange(stmt);
    ResultSet rs = stmt.executeQuery("SELECT * FROM range_1_100 ORDER BY n");
    int i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);

    stmt.setQueryTimeout(10);
    rs = stmt.executeQuery("SELECT * FROM range_1_100 ORDER BY n");
    i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);
  }

  @Test
  public void getGeneratedKeysType() throws SQLException {
    try (java.sql.Statement stmt =
        sharedConn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE,
            ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      stmt.addBatch("DROP TABLE IF EXISTS table0_0;");
      stmt.addBatch("CREATE TABLE table0_0(id INT AUTO_INCREMENT PRIMARY KEY,value INT);");
      stmt.addBatch("INSERT INTO table0_0 VALUES(1, -179653912)");
      stmt.addBatch("INSERT INTO table0_0 VALUES(2, 1207965915)");
      stmt.executeBatch();
      stmt.executeUpdate(
          "INSERT INTO table0_0 VALUES(3, 667711856)", Statement.RETURN_GENERATED_KEYS);
      try (ResultSet rs = stmt.getGeneratedKeys()) {
        Assertions.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
        Assertions.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
        Assertions.assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
        Assertions.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
      }
    }
  }

  @Test
  public void testNegativeFetchSize() throws SQLException {
    try (Statement stmt = sharedConn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS testNegativeFetchSize");
      stmt.execute(
          "CREATE TABLE testNegativeFetchSize(id INT PRIMARY KEY AUTO_INCREMENT,value FLOAT)");
      stmt.addBatch("INSERT INTO testNegativeFetchSize (value)  VALUES(0.05)");
      stmt.addBatch("DELETE FROM testNegativeFetchSize WHERE id <= 2");
      stmt.addBatch("INSERT INTO testNegativeFetchSize (value) VALUES(0.03)");
      stmt.executeBatch();
      try (ResultSet rs = stmt.getGeneratedKeys()) {
        assertThrowsContains(
            SQLSyntaxErrorException.class, () -> rs.setFetchSize(-2), "invalid fetch size -2");
      }
    }
  }

  @Test
  public void largeMaxRows() throws SQLException {
    Statement stmt = createCon().createStatement();
    assertEquals(0L, stmt.getLargeMaxRows());
    try {
      stmt.setLargeMaxRows(-1);
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("max rows cannot be negative"));
    }

    stmt.setLargeMaxRows(10);
    assertEquals(10L, stmt.getLargeMaxRows());

    ensureRange(stmt);
    ResultSet rs = stmt.executeQuery("SELECT * FROM range_1_100 ORDER BY n");
    int i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);

    stmt.setQueryTimeout(10);
    rs = stmt.executeQuery("SELECT * FROM range_1_100 ORDER BY n");
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

    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.unwrap(String.class),
        "he receiver is not a wrapper and does not implement the interface");
    Common.assertThrowsContains(
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
    Statement stmt = sharedConn.createStatement();
    ensureRange(stmt);
    ResultSet rs = stmt.executeQuery("SELECT * FROM range_1_100");
    assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    assertFalse(rs.isClosed());

    rs = stmt.executeQuery("SELECT * FROM range_1_100");
    stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    assertTrue(rs.isClosed());
    stmt.close();
  }

  public void executeTimeOutQeuryWithStatement(Statement stmt) throws SQLException {
    stmt.setQueryTimeout(2);
    assertEquals(2, stmt.getQueryTimeout());
    stmt.execute("SELECT SLEEP(1)");

    SQLNonTransientConnectionException exception =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> {
              stmt.setQueryTimeout(1);
              assertEquals(1, stmt.getQueryTimeout());
              stmt.execute("SELECT SLEEP(2)");
            });
    assertTrue(exception.getMessage().endsWith("query timed out"));
  }

  public void executeTimeOutQeuryWithPrepareStatement(PreparedStatement stmt) throws SQLException {
    stmt.setQueryTimeout(3);
    assertEquals(3, stmt.getQueryTimeout());
    stmt.executeQuery();

    SQLNonTransientConnectionException exception =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> {
              stmt.setQueryTimeout(1);
              assertEquals(1, stmt.getQueryTimeout());
              stmt.executeQuery();
            });

    assertTrue(exception.getMessage().endsWith("query timed out"));
  }

  @Test
  public void queryTimeout() throws Exception {

    // Use-case-1 Test Query Timeout implementation with 'Statement' for 'ClientImpl'.
    Statement stmt = createCon().createStatement();
    Common.assertThrowsContains(
        SQLException.class, () -> stmt.setQueryTimeout(-1), "Query timeout cannot be negative");

    executeTimeOutQeuryWithStatement(stmt);

    // Use-case-2 Test Query Timeout implementation with 'Statement', transactionReplay = true and
    // useServerPrepStmts=false for 'ClientReplayImpl'.
    try (Connection con = createCon("transactionReplay=true&useServerPrepStmts=false")) {
      executeTimeOutQeuryWithStatement(con.createStatement());
    }

    // Use-case-3 Test Query Timeout implementation with 'Statement', transactionReplay = true and
    // useServerPrepStmts=true.
    try (Connection con = createCon("transactionReplay=true&useServerPrepStmts=true")) {
      executeTimeOutQeuryWithStatement(con.createStatement());
    }
    String sql = "SELECT SLEEP(2)";

    // Use-case-6 Test Query Timeout implementation with 'PreparedStatement'
    try (Connection con = (Connection) DriverManager.getConnection(mDefUrl)) {
      executeTimeOutQeuryWithPrepareStatement(con.prepareStatement(sql));
    }

    // Use-case-7 Test Query Timeout implementation with 'ClientPreparedStatement'
    try (Connection con = (Connection) DriverManager.getConnection(mDefUrl)) {
      ClientPreparedStatement clientPrepStmt =
          new ClientPreparedStatement(
              sql,
              con,
              new ReentrantLock(),
              ResultSet.FETCH_FORWARD,
              ResultSet.CONCUR_READ_ONLY,
              Statement.NO_GENERATED_KEYS,
              0);
      executeTimeOutQeuryWithPrepareStatement(clientPrepStmt);
    }

    // Use-case-8 Test Query Timeout implementation with 'ServerPreparedStatement'
    try (Connection con = (Connection) DriverManager.getConnection(mDefUrl)) {
      ServerPreparedStatement serverPrepStmt =
          new ServerPreparedStatement(
              sql,
              con,
              new ReentrantLock(),
              false,
              ResultSet.FETCH_FORWARD,
              ResultSet.CONCUR_READ_ONLY,
              Statement.NO_GENERATED_KEYS,
              0);
      executeTimeOutQeuryWithPrepareStatement(serverPrepStmt);
    }
  }

  @Test
  public void smallQueryTimeout() throws Exception {
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      stmt.setQueryTimeout(0);
      stmt.execute("SELECT 1");

      stmt.setMaxRows(1);
      stmt.execute("SELECT 1");

      stmt.setQueryTimeout(1);
      stmt.execute("SELECT 1");
    } catch (SQLNonTransientConnectionException exception) {
      assertTrue(exception.getMessage().endsWith("query timed out"));
    }
  }

  @Test
  public void escaping() throws Exception {
    try (Connection con =
        (Connection) DriverManager.getConnection(mDefUrl + "&dumpQueriesOnException=true")) {
      Statement stmt = con.createStatement();
      Common.assertThrowsContains(
          SQLException.class,
          () ->
              stmt.executeQuery(
                  "select {fn timestampdiff(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df "),
          "select {fn timestampdiff" + "(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df ");
      stmt.setEscapeProcessing(true);
      Common.assertThrowsContains(
          SQLException.class,
          () ->
              stmt.executeQuery(
                  "select {fn timestampdiff(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df "),
          "select timestampdiff(HOUR, '2003-02-01','2003-05-01') df df ");
    }
  }

  @Test
  public void testWarnings() throws SQLException {
    Statement stmt = sharedConn.createStatement();

    // connection level
    Assertions.assertNull(sharedConn.getWarnings());
    stmt.executeQuery("SAVEPOINT s1");
    SQLWarning warning = sharedConn.getWarnings();
    assertNotNull(warning);
    assertTrue(warning.getMessage().contains("Feature 'SAVEPOINT' is not supported"));
    stmt.executeQuery("SAVEPOINT s1");
    sharedConn.clearWarnings();
    Assertions.assertNull(sharedConn.getWarnings());

    // statement level
    ResultSet rs = stmt.executeQuery("show triggers");
    warning = rs.getWarnings();
    assertNotNull(warning);
    assertTrue(warning.getMessage().contains("Feature 'SHOW TRIGGERS' is not supported"));

    rs = stmt.executeQuery("show triggers");
    rs.clearWarnings();
    Assertions.assertNull(rs.getWarnings());

    stmt.executeQuery("SAVEPOINT s1");
    warning = stmt.getWarnings();
    assertNotNull(warning);
    assertTrue(warning.getMessage().contains("Feature 'SAVEPOINT' is not supported"));

    stmt.executeQuery("SAVEPOINT s1");
    stmt.clearWarnings();
    Assertions.assertNull(stmt.getWarnings());
  }

  @Test
  public void cancel() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.cancel(); // will do nothing

    ExecutorService exec = Executors.newFixedThreadPool(1);

    Common.assertThrowsContains(
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
    Statement stmt = sharedConn.createStatement();
    Common.assertThrowsContains(
        SQLException.class, () -> stmt.setFetchSize(-10), "invalid fetch size");

    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ensureRange(stmt);
    ResultSet rs = stmt.executeQuery("select * FROM range_1_100 ORDER BY n");

    for (int i = 1; i <= 100; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    assertFalse(rs.next());
  }

  @Test
  public void fetchUnFinishedSameStatement() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ensureRange(stmt);
    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM range_1_100 ORDER BY n");

    for (int i = 1; i <= 50; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    ResultSet rs2 = stmt.executeQuery("select * FROM range_1_100 ORDER BY n");

    for (int i = 51; i <= 100; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    for (int i = 1; i <= 100; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void fetchUnFinishedOtherStatement() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ensureRange(stmt);
    stmt.setFetchSize(5);
    assertEquals(5, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM range_1_100 ORDER BY n");

    for (int i = 1; i <= 50; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM range_1_100 ORDER BY n");

    for (int i = 51; i <= 100; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    for (int i = 1; i <= 100; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void fetchUnfinished() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ensureRange(stmt);
    stmt.setFetchSize(1);
    stmt.executeQuery("select * FROM range_1_100 ORDER BY n");
    assertFalse(stmt.getMoreResults());

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs = stmt2.executeQuery("SELECT 1");
    rs.next();
    assertEquals(1, rs.getInt(1));
  }

  @Test
  public void fetchClose() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ensureRange(stmt);
    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM range_1_100 ORDER BY n");

    for (int i = 1; i <= 50; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    stmt.close();
    assertTrue(rs.isClosed());
    stmt.close();

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM range_1_100 ORDER BY n");
    for (int i = 1; i <= 100; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void executeBatchBasic() throws SQLException {
    executeBatchBasic(sharedConn);
    try (Connection con = createCon("allowLocalInfile=true")) {
      executeBatchBasic(con);
    }
  }

  private void executeBatchBasic(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    assertArrayEquals(new int[0], stmt.executeBatch());
    stmt.clearBatch();
    stmt.execute("DROP TABLE IF EXISTS executeBatchBasic");
    stmt.execute(
        "CREATE TABLE executeBatchBasic (t1 int not null primary key auto_increment, t2 int)");

    Common.assertThrowsContains(
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
    assertArrayEquals(new int[0], stmt.executeBatch());
    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (57)");
    stmt.addBatch("WRONG QUERY");
    Common.assertThrowsContains(
        BatchUpdateException.class,
        () -> stmt.executeBatch(),
        "You have an error in your SQL syntax");
  }

  @Test
  public void executeLargeBatchBasic() throws SQLException {
    executeLargeBatchBasic(sharedConn);
    try (Connection con = createCon("allowLocalInfile=true")) {
      executeLargeBatchBasic(con);
    }
  }

  private void executeLargeBatchBasic(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    assertArrayEquals(new long[0], stmt.executeLargeBatch());
    stmt.clearBatch();
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
    ret = stmt.executeLargeBatch();
    Assertions.assertArrayEquals(new long[0], ret);
    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (57)");
    stmt.addBatch("WRONG QUERY");
    Common.assertThrowsContains(
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
    Connection connection = createCon("allowMultiQueries=true");
    String query =
        "SELECT * FROM range_1_100 ORDER BY n LIMIT 10;SELECT * from range_1_100 ORDER BY n;SELECT 2;";
    Statement stmt = connection.createStatement();
    ensureRange(stmt);
    stmt.setFetchSize(3);
    stmt.execute(query);
    assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      assertEquals(i++, rs.getInt(1));
    }
    assertEquals(101, i);
    stmt.setFetchSize(3);

    rs = stmt.executeQuery(query);
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
    rs = stmt.executeQuery(query);
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
    assertEquals(101, i);

    rs = stmt.executeQuery(query);
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

  @Test
  public void testAffectedRow() throws SQLException {
    testAffectedRow(false);
    testAffectedRow(true);
  }

  private void testAffectedRow(boolean useAffectedRows) throws SQLException {
    try (Connection con = createCon("&useAffectedRows=" + useAffectedRows)) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE testAffectedRow");
      stmt.execute("START TRANSACTION");
      stmt.execute("INSERT INTO testAffectedRow values (1), (1), (2), (3)");
      int rowCount = stmt.executeUpdate("UPDATE testAffectedRow set id = 1");
      assertEquals(useAffectedRows ? 2 : 4, rowCount);
      con.rollback();
    }
  }

  @Test
  public void getUpdateCountValueOnFail() throws SQLException {
    try (Statement st = sharedConn.createStatement()) {
      st.execute("DROP TABLE IF EXISTS getUpdateCountValueOnFail");
      try (Statement stmt = sharedConn.createStatement()) {
        assertEquals(-1, stmt.getUpdateCount());
        assertEquals(
            0,
            stmt.executeUpdate(
                "CREATE TABLE getUpdateCountValueOnFail(id VARCHAR(5) PRIMARY KEY,value BOOL)"));
        assertEquals(0, stmt.getUpdateCount());
        try {
          stmt.executeUpdate(
              "CREATE TABLE getUpdateCountValueOnFail(id TINYINT PRIMARY KEY,value SMALLINT");
        } catch (Exception e) {
          // eat
        }
        assertEquals(-1, stmt.getUpdateCount());
      } finally {
        st.execute("DROP TABLE IF EXISTS getUpdateCountValueOnFail");
      }
    }
  }

  @Test
  public void executeGeneratedMultiValues() throws SQLException {
    // normal
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE TABLE executeGenerated");
    assertFalse(
        stmt.execute(
            "INSERT INTO executeGenerated(t2) values (100), (101)",
            Statement.RETURN_GENERATED_KEYS));
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertFalse(rs.next());

    try (PreparedStatement prep =
        sharedConn.prepareStatement(
            "INSERT INTO executeGenerated(t2) values (?), (?)", Statement.RETURN_GENERATED_KEYS)) {
      prep.setInt(1, 104);
      prep.setInt(2, 105);
      prep.execute();
      rs = prep.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertFalse(rs.next());
    }

    // with returnMultiValuesGeneratedIds options
    try (Connection conn = createCon("&returnMultiValuesGeneratedIds")) {
      Statement stmt2 = conn.createStatement();
      assertFalse(
          stmt2.execute(
              "INSERT INTO executeGenerated(t2) values (105), (106)",
              Statement.RETURN_GENERATED_KEYS));
      rs = stmt2.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(6, rs.getInt(1));
      assertFalse(rs.next());

      try (PreparedStatement prep =
          conn.prepareStatement(
              "INSERT INTO executeGenerated(t2) values (?), (?) ON DUPLICATE KEY UPDATE t2=CONCAT(t2,'a')",
              Statement.RETURN_GENERATED_KEYS)) {
        prep.setInt(1, 107);
        prep.setInt(2, 108);
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertFalse(rs.next());
      }
    }

    // with returnMultiValuesGeneratedIds options
    try (Connection conn = createCon("&returnMultiValuesGeneratedIds&useServerPrepStmts")) {
      Statement stmt2 = conn.createStatement();
      assertFalse(
          stmt2.execute(
              "INSERT INTO executeGenerated(t2) values (109), (110)",
              Statement.RETURN_GENERATED_KEYS));
      rs = stmt2.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(9, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(1));
      assertFalse(rs.next());
      try (PreparedStatement prep =
          conn.prepareStatement(
              "INSERT INTO executeGenerated(t2) values (?), (?)",
              Statement.RETURN_GENERATED_KEYS)) {
        prep.setInt(1, 111);
        prep.setInt(2, 112);
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(12, rs.getInt(1));
        assertFalse(rs.next());
      }
      try (PreparedStatement prep =
          conn.prepareStatement(
              "INSERT INTO executeGenerated(t2) values (?), (?) ON DUPLICATE KEY UPDATE t2=CONCAT(t2,'a')",
              Statement.RETURN_GENERATED_KEYS)) {
        prep.setInt(1, 113);
        prep.setInt(2, 114);
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(13, rs.getInt(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void generatedKey() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS tt");
    stmt.execute("CREATE TABLE tt (id int PRIMARY KEY NOT NULL AUTO_INCREMENT, t1 varchar(10))");
    stmt.execute("INSERT INTO tt(t1) VALUES ('t1'), ('t2'), ('t3')");
    stmt.execute("FLUSH TABLES");

    stmt.executeBatch();
    stmt.addBatch("UPDATE tt set t1 = 't-1' WHERE id = 1");
    stmt.addBatch("INSERT INTO tt(t1) VALUES ('t4')");
    stmt.addBatch("INSERT INTO tt(t1) VALUES ('t5')");
    stmt.addBatch("UPDATE tt set t1 = 't-6' WHERE id = 1");
    stmt.executeBatch();
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(5, rs.getInt(1));
    assertFalse(rs.next());

    try (PreparedStatement prep =
        sharedConn.prepareStatement(
            "INSERT IGNORE INTO tt(id, t1) VALUES (?,?)",
            java.sql.Statement.RETURN_GENERATED_KEYS)) {
      prep.setInt(1, 6);
      prep.setString(2, "t6");
      prep.addBatch();
      prep.setInt(1, 7);
      prep.setString(2, "t7");
      prep.addBatch();
      prep.executeBatch();

      rs = prep.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(6, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertFalse(rs.next());
    }
  }
}
