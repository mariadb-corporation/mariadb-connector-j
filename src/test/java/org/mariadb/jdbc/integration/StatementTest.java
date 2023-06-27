// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.plugin.Codec;

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
        "CREATE TABLE bigIntId(`id` bigint(20) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARCHAR(256))");
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
      if (!isXpand()) assertFalse(rs.next());
    }
    try (PreparedStatement ps =
        sharedConn.prepareStatement(
            "UPDATE key_test set id=7 WHERE id=5", Statement.RETURN_GENERATED_KEYS)) {
      ps.execute();
      ResultSet rs = ps.getGeneratedKeys();
      if (!isXpand()) assertFalse(rs.next());
    }

    stmt.execute("DROP TABLE key_test");
  }

  @Test
  public void longGeneratedId() throws SQLException {
    longGeneratedId(BigInteger.ONE);
    longGeneratedId(BigInteger.valueOf(Integer.MAX_VALUE));
    longGeneratedId(BigInteger.valueOf(4294967295L));
    longGeneratedId(BigInteger.valueOf(Long.MAX_VALUE));
  }

  public void longGeneratedId(BigInteger expected) throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("ALTER TABLE bigIntId AUTO_INCREMENT=" + expected.toString());
    stmt.execute(
        "INSERT INTO bigIntId(val) value ('est')", java.sql.Statement.RETURN_GENERATED_KEYS);
    ResultSet rs = stmt.getGeneratedKeys();

    ResultSetMetaData rmeta = rs.getMetaData();
    assertFalse(rmeta.isSigned(1));
    assertTrue(rs.next());
    if (expected.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) >= 1) {
      assertThrowsContains(SQLDataException.class, () -> rs.getInt(1), "integer overflow");
    } else {
      assertEquals(expected.intValueExact(), rs.getInt(1));
    }

    if (expected.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) >= 1) {
      assertThrowsContains(
          SQLDataException.class, () -> rs.getLong(1), "cannot be decoded as Long");
    } else {
      assertEquals(expected.longValueExact(), rs.getLong(1));
    }
    assertTrue(expected.compareTo(((CompleteResult) rs).getBigInteger(1)) == 0);
    assertTrue(new BigDecimal(expected).compareTo(rs.getBigDecimal(1)) == 0);
  }

  @Test
  public void unsignedMetadataResult() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS unsignedMetadataResult");
    stmt.execute(
        "CREATE TABLE unsignedMetadataResult("
            + "c0 TINYINT UNSIGNED, "
            + "c1 SMALLINT UNSIGNED, "
            + "c2 MEDIUMINT UNSIGNED, "
            + "c3 INTEGER UNSIGNED, "
            + "c4 BIGINT UNSIGNED, "
            + "c5 DOUBLE UNSIGNED, "
            + "c6 FLOAT UNSIGNED, "
            + "c7 DECIMAL UNSIGNED)");
    stmt.execute("INSERT INTO unsignedMetadataResult VALUES(10,11,12,13,14,15,16,17)");
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
    if (!isXpand()) {
      assertFalse(stmt.execute("DO 1"));
      Assertions.assertNull(stmt.getResultSet());
      assertEquals(0, stmt.getUpdateCount());
      assertFalse(stmt.getMoreResults());
      assertEquals(-1, stmt.getUpdateCount());
    }

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

    SQLException e = Assertions.assertThrows(SQLException.class, stmt::getGeneratedKeys);
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
    if (!isXpand()) {
      assertEquals(0, stmt.executeUpdate("DO 1"));
    }
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
    if (!isXpand()) {
      assertEquals(0, stmt.executeLargeUpdate("DO 1"));
    }
  }

  @Test
  public void executeQuery() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1");
    assertTrue(rs.next());
    if (!isXpand()) {
      rs = stmt.executeQuery("DO 1");
      assertFalse(rs.next());
    }
  }

  @Test
  public void close() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertFalse(stmt.isClosed());
    ResultSet rs = stmt.executeQuery("select * FROM sequence_1_to_10 LIMIT 1");
    rs.next();
    rs.getObject(1);

    rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10000");
    assertFalse(rs.isClosed());
    stmt.close();
    assertTrue(stmt.isClosed());
    assertTrue(rs.isClosed());

    Common.assertThrowsContains(
        SQLException.class, stmt::clearBatch, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::isPoolable, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.setPoolable(true),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        stmt::closeOnCompletion,
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        stmt::isCloseOnCompletion,
        "Cannot do an operation on a closed statement");

    Common.assertThrowsContains(
        SQLException.class,
        stmt::getResultSetConcurrency,
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::getFetchSize, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::getMoreResults, "Cannot do an operation on a closed statement");
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
        SQLException.class, stmt::executeBatch, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::getConnection, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.getMoreResults(1),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::cancel, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::getMaxRows, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::getLargeMaxRows, "Cannot do an operation on a closed statement");

    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.setMaxRows(1),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt.setEscapeProcessing(true),
        "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::getQueryTimeout, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class, stmt::getUpdateCount, "Cannot do an operation on a closed statement");
    Common.assertThrowsContains(
        SQLException.class,
        stmt::getLargeUpdateCount,
        "Cannot do an operation on a closed statement");
  }

  @Test
  public void maxRows() throws SQLException {
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

    ResultSet rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10000");
    int i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);

    stmt.setQueryTimeout(2);
    rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10000");
    i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);
  }

  @Test
  public void largeMaxRows() throws SQLException {
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

    ResultSet rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10000");
    int i = 0;
    while (rs.next()) {
      i++;
      assertEquals(i, rs.getInt(1));
    }
    assertEquals(10, i);

    stmt.setQueryTimeout(2);
    rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10000");
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
    ResultSet rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10000");
    assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    assertFalse(rs.isClosed());

    rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10000");
    stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    assertTrue(rs.isClosed());
    stmt.close();
  }

  @Test
  @Timeout(20)
  public void queryTimeout() {
    Assumptions.assumeTrue(
        isMariaDBServer()
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !isXpand());
    Statement stmt = sharedConn.createStatement();

    Common.assertThrowsContains(
        SQLException.class, () -> stmt.setQueryTimeout(-1), "Query timeout cannot be negative");

    Common.assertThrowsContains(
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
  public void smallQueryTimeout() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.setQueryTimeout(1);
    stmt.execute("SELECT 1");

    stmt.setMaxRows(1);
    stmt.execute("SELECT 1");

    stmt.setQueryTimeout(0);
    stmt.execute("SELECT 1");
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
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(isMariaDBServer() && !isXpand());

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
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !isXpand());
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
    Statement stmt2 = sharedConn.createStatement();
    Common.assertThrowsContains(
        SQLException.class, () -> stmt.setFetchSize(-10), "invalid fetch size");

    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM sequence_1_to_10000");

    for (int i = 1; i <= 10000; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    assertFalse(rs.next());

    rs = stmt.executeQuery("select * FROM sequence_1_to_10");
    ResultSet rs2 = stmt2.executeQuery("SELECT 200");
    for (int i = 1; i <= 10; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertTrue(rs2.next());
    assertEquals(200, rs2.getInt(1));
  }

  @Test
  public void fetchUnFinishedSameStatement() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM sequence_1_to_10000");

    for (int i = 1; i <= 5000; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    ResultSet rs2 = stmt.executeQuery("select * FROM sequence_1_to_10000");

    for (int i = 5001; i <= 10000; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    for (int i = 1; i <= 10000; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void fetchUnFinishedOtherStatement() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(5);
    assertEquals(5, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM sequence_1_to_10000");

    for (int i = 1; i <= 10; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM sequence_1_to_10000");

    for (int i = 11; i <= 10000; i++) {
      assertTrue(rs.next(), "val " + i);
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    for (int i = 1; i <= 10000; i++) {
      assertTrue(rs2.next());
      assertEquals(i, rs2.getInt(1));
    }
    assertFalse(rs2.next());
  }

  @Test
  public void fetchUnfinished() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(1);
    stmt.executeQuery("select * FROM sequence_1_to_10");
    assertFalse(stmt.getMoreResults());

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs = stmt2.executeQuery("SELECT 1");
    rs.next();
    assertEquals(1, rs.getInt(1));
  }

  @Test
  public void fetchClose() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(10);
    assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM sequence_1_to_10000");

    for (int i = 1; i <= 5000; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    stmt.close();
    assertTrue(rs.isClosed());
    stmt.close();

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM sequence_1_to_10000");
    for (int i = 1; i <= 10000; i++) {
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
    try {
      stmt.executeBatch();
      fail();
    } catch (BatchUpdateException e) {
      assertTrue(
          e.getMessage().contains("You have an error in your SQL syntax")
              || e.getMessage().contains("syntax error"));
      assertNotNull(e.getCause());
      assertEquals(e.getCause().getMessage(), e.getMessage());
      assertEquals(((SQLException) e.getCause()).getSQLState(), e.getSQLState());
      assertEquals(((SQLException) e.getCause()).getErrorCode(), e.getErrorCode());
    }
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
    try {
      stmt.executeBatch();
      fail();
    } catch (BatchUpdateException e) {
      assertTrue(
          e.getMessage().contains("You have an error in your SQL syntax")
              || e.getMessage().contains("syntax error"));
      assertNotNull(e.getCause());
      assertEquals(e.getCause().getMessage(), e.getMessage());
      assertEquals(((SQLException) e.getCause()).getSQLState(), e.getSQLState());
      assertEquals(((SQLException) e.getCause()).getErrorCode(), e.getErrorCode());
    }
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
    // error MXS-3929 for maxscale 6.2.0
    Assumptions.assumeTrue(
        !sharedConn.getMetaData().getDatabaseProductVersion().contains("maxScale-6.2.0"));
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multi");
    stmt.setFetchSize(3);
    stmt.execute(
        "CREATE PROCEDURE multi() BEGIN SELECT * from sequence_1_to_10; SELECT * FROM sequence_1_to_10000;SELECT 2; END");
    stmt.execute("CALL multi()");
    assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      assertEquals(i++, rs.getInt(1));
    }
    assertEquals(10001, i);
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
    assertEquals(10001, i);

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

  @Test
  public void testAffectedRow() throws SQLException {
    testAffectedRow(false);
    if (!isXpand()) {
      testAffectedRow(true);
    }
  }

  @Test
  public void ensureClassDefined() {
    for (Codec<?> codec : sharedConn.getContext().getConf().codecs()) {
      Type it = codec.getClass().getGenericInterfaces()[0];
      ParameterizedType parameterizedType = (ParameterizedType) it;
      Type typeParameter = parameterizedType.getActualTypeArguments()[0];
      if (!"byte[]".equals(codec.className()))
        assertEquals(((Class<?>) typeParameter).getName(), codec.className());
    }
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
  public void statementIdentifier() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertTrue(stmt.isSimpleIdentifier("good_$one"));
    assertTrue(stmt.isSimpleIdentifier("anotherçone"));
    assertFalse(stmt.isSimpleIdentifier("another'çone"));
    assertFalse(stmt.isSimpleIdentifier(null));
    assertFalse(stmt.isSimpleIdentifier(""));
  }

  @Test
  public void statementEnquoteIdentifier() throws SQLException {
    Statement stmt = sharedConn.createStatement();

    assertEquals("good_$one", stmt.enquoteIdentifier("good_$one", false));
    assertEquals("`good_$one`", stmt.enquoteIdentifier("good_$one", true));
    assertEquals("`good_$one`", stmt.enquoteIdentifier("`good_$one`", true));
    assertEquals("`🌟s`", stmt.enquoteIdentifier("🌟s", false));
    assertEquals("`🌟s`", stmt.enquoteIdentifier("`🌟s`", false));
    assertEquals("`good_``è``one`", stmt.enquoteIdentifier("good_`è`one", false));
    try {
      stmt.enquoteIdentifier("\u0000ff", true);
      fail("must have thrown exception");
    } catch (SQLException e) {
      // expected
    }
  }

  @Test
  public void statementEnquoteString() throws SQLException {
    Statement stmt = sharedConn.createStatement();

    assertEquals("'good_$one'", stmt.enquoteLiteral("good_$one"));
    assertEquals(
        "'another\\Z\\'\\\"one\\n \\b test'", stmt.enquoteLiteral("another\u001A'\"one\n \b test"));
  }

  @Test
  public void statementEnquoteNCharLiteral() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    assertEquals("N'good''one'", stmt.enquoteNCharLiteral("good'one"));
  }
}
