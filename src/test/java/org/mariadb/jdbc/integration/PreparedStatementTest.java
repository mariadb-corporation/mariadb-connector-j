// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Random;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class PreparedStatementTest extends Common {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepare1");
    stmt.execute("DROP TABLE IF EXISTS prepare2");
    stmt.execute("DROP TABLE IF EXISTS prepare3");
    stmt.execute("DROP TABLE IF EXISTS prepare4");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE prepare1 (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("CREATE TABLE prepare2 (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("CREATE TABLE prepare3 (t1 LONGTEXT, t2 LONGTEXT, t3 LONGTEXT, t4 LONGTEXT)");
    stmt.execute("CREATE TABLE prepare4 (t1 int)");
    stmt.execute("INSERT INTO prepare4 VALUES (1),(2),(3),(4),(5)");
  }

  @Test
  public void prep() throws SQLException {
    try (PreparedStatement stmt = sharedConn.prepareStatement("SELECT ?")) {
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }

    try (PreparedStatement stmt =
        sharedConn.prepareStatement(
            "SELECT ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }

    try (PreparedStatement stmt =
        sharedConn.prepareStatement(
            "SELECT ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE,
            ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
      // not supported
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }
  }

  @Test
  public void execute() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      execute(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      execute(con);
    }
  }

  private void execute(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        conn.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      assertFalse(preparedStatement.execute());

      ParameterMetaData paramMeta = preparedStatement.getParameterMetaData();
      paramMeta.getParameterTypeName(1);
      paramMeta = preparedStatement.getParameterMetaData();
      paramMeta.getParameterTypeName(1);

      // verification
      ResultSet rs = stmt.executeQuery("SELECT * FROM prepare1");
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertFalse(rs.next());

      // prepare is already done. must only execute.
      preparedStatement.setInt(1, 7);
      preparedStatement.setInt(2, 12);
      assertFalse(preparedStatement.execute());

      rs = stmt.executeQuery("SELECT * FROM prepare1 WHERE t1 > 5");
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertEquals(12, rs.getInt(2));
      assertFalse(rs.next());
    }

    try (PreparedStatement preparedStatement =
        conn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 4);
      assertTrue(preparedStatement.execute());
      ResultSet rs = preparedStatement.getResultSet();
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertEquals(12, rs.getInt(2));
      assertFalse(rs.next());

      preparedStatement.setMaxRows(1);
      preparedStatement.setInt(1, 4);
      assertTrue(preparedStatement.execute());
      rs = preparedStatement.getResultSet();
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      if (isMariaDBServer()) {
        // setMaxRows() has no effect for mysql, since not supporting SET STATEMENT SQL_SELECT_LIMIT
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void executeWithoutAllParameters() throws SQLException {
    executeWithoutAllParameters(sharedConn);
    executeWithoutAllParameters(sharedConnBinary);
  }

  public void executeWithoutAllParameters(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(2, 10);
      Common.assertThrowsContains(
          SQLException.class,
          preparedStatement::executeUpdate,
          "Parameter at position 1 is not " + "set");

      preparedStatement.setNull(1, Types.VARBINARY);
      preparedStatement.executeUpdate();
      ResultSet rs = stmt.executeQuery("SELECT * FROM prepare1");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void executeUpdate() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      assertEquals(1, preparedStatement.executeUpdate());

      // verification that query without resultset return an empty resultset
      preparedStatement.clearParameters();
      Common.assertThrowsContains(
          SQLException.class,
          preparedStatement::executeQuery,
          "Parameter at position 1 is not set");
      preparedStatement.setInt(2, 11);
      preparedStatement.setInt(1, 6);
      ResultSet rs0 = preparedStatement.executeQuery();
      assertFalse(rs0.next());

      // verification
      ResultSet rs = stmt.executeQuery("SELECT * FROM prepare1");
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(6, rs.getInt(1));
      assertEquals(11, rs.getInt(2));
      assertFalse(rs.next());
    }

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1")) {
      Common.assertThrowsContains(
          SQLException.class,
          preparedStatement::executeUpdate,
          "the given SQL statement produces an unexpected ResultSet object");
    }
  }

  @Test
  public void executeQuery() throws SQLException {
    executeQuery(sharedConn);
    executeQuery(sharedConnBinary);
    try (Connection con = createCon("useServerPrepStmts=true&enableSkipMeta=false")) {
      executeQuery(con);
    }
  }

  @Test
  public void tryMaybeNotPreparable() throws SQLException {
    try (Connection con = createCon("useServerPrepStmts")) {
      try (PreparedStatement prep = con.prepareStatement("CREATE TABLE maybeCreate(id int)")) {
        prep.execute();
      }
    } finally{
      sharedConn.createStatement().execute("DROP TABLE IF EXISTS maybeCreate");
    }
    try (Connection con = createCon("useServerPrepStmts")) {
      try (PreparedStatement prep =
                   con.prepareStatement(
                           "CREATE PROCEDURE maybeProc(IN  I date) BEGIN SELECT I; END")) {
        prep.execute();
      }
    } finally{
      sharedConn.createStatement().execute("DROP PROCEDURE IF EXISTS maybeProc");
    }
  }

  private void executeQuery(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute(
        "CREATE TEMPORARY TABLE prepare10 (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("INSERT INTO prepare10(t1, t2) VALUES (5,10), (40,20), (127,45)");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("SELECT * FROM prepare10 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 20);
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(40, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(127, rs.getInt(1));
      assertEquals(45, rs.getInt(2));
      assertFalse(rs.next());
      Common.assertThrowsContains(
          SQLException.class, () -> preparedStatement.setInt(-20, 2), "wrong parameter index -20");

      preparedStatement.setInt(1, 50);
      rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(127, rs.getInt(1));
      assertEquals(45, rs.getInt(2));
      assertFalse(rs.next());
      Common.assertThrowsContains(
          SQLException.class, () -> preparedStatement.setInt(-20, 2), "wrong parameter index -20");
      stmt.execute("ALTER TABLE prepare10 ADD COLUMN t3 varchar(20) default 'tt'");
      preparedStatement.setInt(1, 20);
      rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(40, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertEquals("tt", rs.getString(3));
    }

    try (PreparedStatement preparedStatement =
        con.prepareStatement("SELECT * FROM prepare10 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 20);
      preparedStatement.executeQuery();
    }
  }

  @Test
  public void clearParameters() throws Exception {
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      clearParameters(con);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      clearParameters(con);
    }
  }

  public void clearParameters(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      preparedStatement.clearParameters();
      assertThrows(SQLException.class, preparedStatement::execute);
    }
  }

  @Test
  public void closeOnCompletion() throws SQLException {
    PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?");
    Assertions.assertFalse(preparedStatement.isCloseOnCompletion());
    preparedStatement.closeOnCompletion();
    Assertions.assertTrue(preparedStatement.isCloseOnCompletion());
    Assertions.assertFalse(preparedStatement.isClosed());
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    Assertions.assertFalse(preparedStatement.isClosed());
    rs.close();
    Assertions.assertTrue(rs.isClosed());
    Assertions.assertTrue(preparedStatement.isClosed());
  }

  @Test
  public void executeBatch() throws SQLException {
    executeBatch(sharedConn);
    executeBatch(sharedConnBinary);
    try (Connection con = createCon("allowLocalInfile=true")) {
      executeBatch(con);
    }
    try (Connection con = createCon("allowLocalInfile=true&useServerPrepStmts=true")) {
      executeBatch(con);
    }
  }

  private void executeBatch(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {

      try (PreparedStatement preparedStatement2 =
          con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
        preparedStatement2.setInt(1, 15);
        preparedStatement2.setInt(2, 110);
        preparedStatement2.addBatch();
        preparedStatement2.executeBatch();
      }

      int[] res = preparedStatement.executeBatch();
      assertEquals(0, res.length);
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      preparedStatement.addBatch();
      res = preparedStatement.executeBatch();
      assertEquals(1, res.length);
      res = preparedStatement.executeBatch();
      assertEquals(0, res.length);
    }

    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 40);
      preparedStatement.setInt(2, 20);
      preparedStatement.addBatch();
      preparedStatement.setInt(1, 127);
      preparedStatement.setInt(2, 45);
      preparedStatement.addBatch();
      int[] res = preparedStatement.executeBatch();
      assertEquals(2, res.length);
    }

    try (PreparedStatement preparedStatement =
        con.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 20);
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(40, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(127, rs.getInt(1));
      assertEquals(45, rs.getInt(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void executeWrongBatch() throws SQLException {
    executeWrongBatch(sharedConn);
    executeWrongBatch(sharedConnBinary);
    try (Connection con = createCon("useBulkStmts=false&useServerPrepStmts=true")) {
      executeWrongBatch(con);
    }
  }

  private void executeWrongBatch(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE prepare1");
    stmt.execute("SET sql_mode = concat(@@sql_mode,',ERROR_FOR_DIVISION_BY_ZERO')");
    try (PreparedStatement preparedStatement = con.prepareStatement("SELECT 5/?")) {
      preparedStatement.setInt(1, 5);
      preparedStatement.addBatch();
      preparedStatement.executeBatch();

      preparedStatement.setInt(1, 5);
      preparedStatement.addBatch();
      preparedStatement.setInt(1, 0);
      preparedStatement.addBatch();
      try {
        preparedStatement.executeBatch();
      } catch (BatchUpdateException e) {
        // eat
      }
    }
  }

  @Test
  public void executeBatchMultiple() throws SQLException {
    try (Connection con = createCon("allowMultiQueries&useBulkStmts=false")) {
      executeBatchMultiple(con);
    }
  }

  private void executeBatchMultiple(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?);DO 1")) {
      int[] res = preparedStatement.executeBatch();
      assertEquals(0, res.length);
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      preparedStatement.addBatch();
      res = preparedStatement.executeBatch();
      assertEquals(1, res.length);
      res = preparedStatement.executeBatch();
      assertEquals(0, res.length);
    }

    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?);DO 1")) {
      preparedStatement.setInt(1, 40);
      preparedStatement.setInt(2, 20);
      preparedStatement.addBatch();
      preparedStatement.setInt(1, 127);
      preparedStatement.setInt(2, 45);
      preparedStatement.addBatch();
      int[] res = preparedStatement.executeBatch();
      assertEquals(2, res.length);
    }

    try (PreparedStatement preparedStatement =
        con.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 20);
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(40, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(127, rs.getInt(1));
      assertEquals(45, rs.getInt(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void executeLargeBatch() throws SQLException {
    executeLargeBatch(sharedConn);
    executeLargeBatch(sharedConnBinary);
    try (Connection con = createCon("allowLocalInfile=true")) {
      executeLargeBatch(con);
    }
    try (Connection con = createCon("allowLocalInfile=true&useServerPrepStmts=true")) {
      executeLargeBatch(con);
    }
  }

  private void executeLargeBatch(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.executeLargeBatch();
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      preparedStatement.addBatch();
      preparedStatement.executeLargeBatch();
      preparedStatement.executeLargeBatch();
    }

    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 40);
      preparedStatement.setInt(2, 20);
      preparedStatement.addBatch();
      preparedStatement.setInt(1, 127);
      preparedStatement.setInt(2, 45);
      preparedStatement.addBatch();
      preparedStatement.executeLargeBatch();
    }

    try (PreparedStatement preparedStatement =
        con.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 20);
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(40, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(127, rs.getInt(1));
      assertEquals(45, rs.getInt(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void executeBatchGenerated() throws SQLException {
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "INSERT INTO prepare2(t2) VALUES (?)", java.sql.Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setInt(1, 10);
      preparedStatement.addBatch();
      preparedStatement.setInt(1, 20);
      preparedStatement.addBatch();
      preparedStatement.executeBatch();
      ResultSet rs = preparedStatement.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertFalse(rs.next());
    }

    try (PreparedStatement preparedStatement =
        sharedConnBinary.prepareStatement(
            "INSERT INTO prepare2(t2) VALUES (?)", java.sql.Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setInt(1, 10);
      preparedStatement.addBatch();
      preparedStatement.setInt(1, 20);
      preparedStatement.addBatch();
      preparedStatement.executeBatch();
      ResultSet rs = preparedStatement.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));
      assertFalse(rs.next());
    }

    try (Connection con = createCon("allowMultiQueries")) {
      try (PreparedStatement preparedStatement =
          con.prepareStatement(
              "INSERT INTO prepare2(t2) VALUES (?);INSERT INTO prepare2(t2) VALUES (?)",
              java.sql.Statement.RETURN_GENERATED_KEYS)) {
        preparedStatement.setInt(1, 30);
        preparedStatement.setInt(2, 50);
        preparedStatement.execute();
        ResultSet rs = preparedStatement.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        assertFalse(rs.next());

        preparedStatement.setInt(1, 210);
        preparedStatement.setInt(2, 110);
        preparedStatement.addBatch();
        preparedStatement.setInt(1, 220);
        preparedStatement.setInt(2, 220);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        rs = preparedStatement.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void emptyExecuteBatch() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    stmt.execute("INSERT INTO prepare1(t1, t2) VALUES (5,10), (40,20), (127,45)");
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      assertEquals(0, preparedStatement.executeBatch().length);
    }
    try (PreparedStatement preparedStatement =
        sharedConnBinary.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      assertEquals(0, preparedStatement.executeBatch().length);
    }
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      assertEquals(0, preparedStatement.executeLargeBatch().length);
    }
    try (PreparedStatement preparedStatement =
        sharedConnBinary.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      assertEquals(0, preparedStatement.executeLargeBatch().length);
    }
  }

  @Test
  public void moreResults() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      moreResults(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      moreResults(con);
    }
  }

  private void moreResults(Connection con) throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = con.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multi");
    stmt.setFetchSize(3);
    stmt.execute(
        "CREATE PROCEDURE multi() BEGIN SELECT * from seq_1_to_10; SELECT * FROM seq_1_to_1000;SELECT 2; END");
    stmt.execute("CALL multi()");
    Assertions.assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(1001, i);
    stmt.setFetchSize(3);
    PreparedStatement prep = con.prepareStatement("CALL multi()");
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults());
    Assertions.assertTrue(rs.isClosed());
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }

    prep.setFetchSize(3);
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
    Assertions.assertFalse(rs.isClosed());
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(11, i);
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(1001, i);

    rs = prep.executeQuery();
    prep.close();
    assertTrue(rs.isClosed());
  }

  @Test
  public void moreRowLimitedResults() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      moreRowLimitedResults(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      moreRowLimitedResults(con);
    }
  }

  private void moreRowLimitedResults(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multi");
    stmt.setFetchSize(3);
    stmt.setMaxRows(5);
    stmt.execute(
        "CREATE PROCEDURE multi() BEGIN SELECT * from prepare4; SELECT * FROM prepare4;SELECT 2; END");
    stmt.execute("CALL multi()");
    Assertions.assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(6, i);
    stmt.setFetchSize(3);
    PreparedStatement prep = con.prepareStatement("CALL multi()");
    prep.setMaxRows(20);
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults());
    Assertions.assertTrue(rs.isClosed());
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }

    prep.setFetchSize(3);
    prep.setMaxRows(5);
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
    Assertions.assertFalse(rs.isClosed());
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(6, i);
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(6, i);

    rs = prep.executeQuery();
    prep.close();
    assertTrue(rs.isClosed());
  }

  @Test
  public void prepareWithError() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      prepareWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      prepareWithError(con);
    }
  }

  private void prepareWithError(Connection con) throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepareError");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE prepareError(id int primary key, val varchar(10))");
    stmt.execute("INSERT INTO prepareError(id, val) values (1, 'val1')");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO prepareError(id, val) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "val2");
      Common.assertThrowsContains(
          SQLException.class, prep::execute, "Duplicate entry '1' for key 'PRIMARY'");
    }
    try (PreparedStatement prep = con.prepareStatement("Wrong command")) {
      Common.assertThrowsContains(
          SQLException.class, prep::execute, "You have an error in your SQL syntax");
    }
  }

  @Test
  public void streamNotFinished() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(2);
    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10");

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("SELECT 1");
    rs2.next();
    assertEquals(1, rs2.getInt(1));
    for (int i = 1; i <= 10; i++) {
      rs.next();
      assertEquals(i, rs.getInt(1));
    }
  }

  @Test
  public void expectedError() throws SQLException {
    try (PreparedStatement prep = sharedConn.prepareStatement("SELECT ?")) {
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.addBatch("SELECT 1"),
          "addBatch(String sql) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1"),
          "execute(String sql) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1", Statement.NO_GENERATED_KEYS),
          "execute(String sql, int autoGeneratedKeys) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1", new int[] {}),
          "execute(String sql, int[] columnIndexes) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1", new String[] {}),
          "execute(String sql, String[] columnNames) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeQuery("SELECT 1"),
          "executeQuery(String sql) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1"),
          "executeUpdate(String sql) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1", Statement.NO_GENERATED_KEYS),
          "executeUpdate(String sql, int autoGeneratedKeys) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1", new int[] {}),
          "executeUpdate(String sql, int[] columnIndexes) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1", new String[] {}),
          "executeUpdate(String sql, String[] columnNames) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1"),
          "executeLargeUpdate(String sql) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1", Statement.NO_GENERATED_KEYS),
          "executeLargeUpdate(String sql, int autoGeneratedKeys) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1", new int[] {}),
          "executeLargeUpdate(String sql, int[] columnIndexes) cannot be called on preparedStatement");
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1", new String[] {}),
          "executeLargeUpdate(String sql, String[] columnNames) cannot be called on preparedStatement");
    }
  }

  @Test
  public void largeMaxRows() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      largeMaxRows(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      largeMaxRows(con);
    }
  }

  private void largeMaxRows(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS largeMaxRows");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE largeMaxRows(id int)");
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO largeMaxRows(id) VALUE (?)")) {
      for (int i = 1; i < 51; i++) {
        prep.setInt(1, i);
        prep.execute();
      }
    }

    try (PreparedStatement prep = con.prepareStatement("SELECT * FROM largeMaxRows")) {
      assertEquals(0L, prep.getLargeMaxRows());
      ResultSet rs = prep.executeQuery();
      int i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(50, i);

      try {
        prep.setLargeMaxRows(-1);
        Assertions.fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("max rows cannot be negative"));
      }
      prep.setLargeMaxRows(10);
      assertEquals(10L, prep.getLargeMaxRows());

      rs = prep.executeQuery();
      i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(10, i);

      prep.setQueryTimeout(2);
      rs = prep.executeQuery();
      i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(10, i);

      prep.setQueryTimeout(20);
      prep.setLargeMaxRows(0);
      rs = prep.executeQuery();
      i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(50, i);
      prep.setQueryTimeout(0);
      prep.setQueryTimeout(0);
    }
  }

  @Test
  public void largeMaxRowsBatch() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      largeMaxRowsBatch(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      largeMaxRowsBatch(con);
    }
  }

  private void prepareInsert(PreparedStatement prep) throws SQLException {
    prep.setInt(1, 0);
    prep.addBatch();
    prep.setInt(1, 1);
    prep.addBatch();
    prep.executeBatch();
  }

  private void largeMaxRowsBatch(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS large_max_rows_batch");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE large_max_rows_batch(id int)");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO large_max_rows_batch(id) VALUE (?)")) {
      prepareInsert(prep);

      prep.setMaxRows(1);
      prepareInsert(prep);

      prep.setQueryTimeout(1);
      prepareInsert(prep);

      prep.setMaxRows(0);
      prepareInsert(prep);

      prep.setLargeMaxRows(2);
      prepareInsert(prep);

      prep.setQueryTimeout(0);
      prepareInsert(prep);
    }
    ResultSet rs = stmt.executeQuery("SELECT count(*) FROM large_max_rows_batch");
    rs.next();
    assertEquals(12, rs.getInt(1));
  }

  @Test
  public void decrementCache() throws SQLException {
    decrementCache("&useServerPrepStmts=true&prepStmtCacheSize=5");
    decrementCache("&useServerPrepStmts=true&useServerPrepStmts=false");
  }

  public void decrementCache(String connString) throws SQLException {
    try (Connection con = createCon(connString)) {
      PreparedStatement prep = con.prepareStatement("SELECT 1");
      prep.execute();
      PreparedStatement prep2 = con.prepareStatement("SELECT 1");
      prep2.execute();

      for (int i = 1; i < 10; i++) {
        try (PreparedStatement prep1 = con.prepareStatement("SELECT " + i)) {
          prep1.execute();
          prep1.setQueryTimeout(1);
        }
      }

      prep.setQueryTimeout(1); // will close prepare
      prep.setQueryTimeout(2); // will close prepare
      prep.execute();
      prep.close();
      prep2.close();
    }
  }

  @Test
  public void prepareStatementConcur() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      prepareStatementConcur(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      prepareStatementConcur(con);
    }
    try (Connection con = createCon("&useServerPrepStmts=false&disablePipeline")) {
      prepareStatementConcur(con);
    }
  }

  private void prepareStatementConcur(Connection con) throws SQLException {
    try (PreparedStatement prep = con.prepareStatement("SELECT 1", new int[] {})) {
      prep.execute();
    }

    try (PreparedStatement prep = con.prepareStatement("SELECT 1", new String[] {})) {
      prep.execute();
    }

    try (PreparedStatement prep =
        con.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS)) {
      assertEquals(ResultSet.CONCUR_READ_ONLY, prep.getResultSetConcurrency());
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, prep.getResultSetType());
      prep.execute();
    }
    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT 1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
      assertEquals(ResultSet.CONCUR_UPDATABLE, prep.getResultSetConcurrency());
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, prep.getResultSetType());

      prep.execute();
    }
  }

  @Test
  public void more2BytesParameters() throws Throwable {
    int[] rnds = new int[100000];
    StringBuilder sb = new StringBuilder("select ?");
    for (int i = 0; i < 100000; i++) {
      rnds[i] = (int) (Math.random() * 1000);
    }
    for (int i = 1; i < 100000; i++) {
      sb.append(",?");
    }
    String sql = sb.toString();

    try (PreparedStatement st = sharedConnBinary.prepareStatement(sql)) {
      for (int i = 1; i <= 100000; i++) {
        st.setInt(i, rnds[i - 1]);
      }
      Common.assertThrowsContains(
          SQLException.class,
          st::executeQuery,
          "Prepared statement contains too many placeholders");
    }
    assertTrue(sharedConnBinary.isValid(1));
  }

  private String generateLongText(int len) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    Random random = new Random();
    return random
        .ints(leftLimit, rightLimit)
        .limit(len)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  @Test
  public void skippingRes() throws SQLException {
    int maxAllowedPacket = getMaxAllowedPacket();
    Assumptions.assumeTrue(maxAllowedPacket > 35_000_000);
    skippingRes(sharedConn);
    skippingRes(sharedConnBinary);
    try (Connection compressText =
        createCon("useCompression&maxAllowedPacket=" + maxAllowedPacket)) {
      skippingRes(compressText);
    }
    try (Connection compressBinary =
        createCon("useCompression&useServerPrepStmts&maxAllowedPacket=" + maxAllowedPacket)) {
      skippingRes(compressBinary);
    }
  }

  private void skippingRes(java.sql.Connection con) throws SQLException {
    con.createStatement().execute("TRUNCATE prepare3");
    String longText = generateLongText(20_000_000);
    String mediumText = generateLongText(10_000_000);
    String smallIntText = generateLongText(60_000);

    try (PreparedStatement prep = con.prepareStatement("INSERT INTO prepare3 values (?,?,?,?)")) {
      prep.setString(1, longText);
      prep.setString(2, mediumText);
      prep.setString(3, smallIntText);
      prep.setString(4, "expected");
      prep.execute();
    }

    try (PreparedStatement prep = con.prepareStatement("SELECT * FROM prepare3")) {
      ResultSet rs = prep.executeQuery();
      rs.next();
      assertEquals("expected", rs.getString(4));
      assertEquals(smallIntText, rs.getString(3));
      assertEquals(mediumText, rs.getString(2));
      assertEquals(longText, rs.getString(1));
    }
  }

  @Test
  public void wrongPosition() throws SQLException {
    try (PreparedStatement prep = sharedConn.prepareStatement("SELECT 1 FROM DUAL WHERE 0=1")) {
      ResultSet rs = prep.executeQuery();
      Common.assertThrowsContains(SQLException.class, () -> rs.getString(1), "wrong row position");
      Common.assertThrowsContains(
          SQLException.class, () -> rs.getString("1"), "wrong row position");
      Common.assertThrowsContains(
          SQLException.class, () -> rs.getObject(1, String.class), "wrong row position");
    }
    try (PreparedStatement prep = sharedConn.prepareStatement("SELECT 1 FROM DUAL")) {
      ResultSet rs = prep.executeQuery();
      rs.next();
      Common.assertThrowsContains(
          SQLException.class,
          () -> rs.getString(-1),
          "Wrong index position. Is -1 but must be in 1-1 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> rs.getString(10),
          "Wrong index position. Is 10 but must be in 1-1 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> rs.getObject(-1, String.class),
          "Wrong index position. Is -1 but must be in 1-1 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> rs.getObject(10, String.class),
          "Wrong index position. Is 10 but must be in 1-1 range");
    }
  }
}
