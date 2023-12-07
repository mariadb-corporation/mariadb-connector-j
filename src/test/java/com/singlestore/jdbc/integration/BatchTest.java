// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.util.ClientParser;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BatchTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    after2();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BatchTest");
    stmt.execute(
        "CREATE TABLE BatchTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");
    stmt.execute("CREATE TABLE timestampCal(id int, val TIMESTAMP)");
  }

  @AfterAll
  public static void after2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS timestampCal");
    stmt.execute("DROP TABLE IF EXISTS BatchTest");
  }

  @Test
  public void batchClear() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS batchClear");
    stmt.execute("CREATE TABLE batchClear(c0 VARCHAR(16))");
    try (PreparedStatement prep =
        sharedConn.prepareStatement("INSERT INTO batchClear VALUES (?)")) {
      prep.setString(1, "1");
      prep.addBatch();

      prep.setString(1, "2");
      prep.addBatch();

      prep.setString(1, "3");
      prep.addBatch();

      prep.executeBatch();

      prep.setString(1, "4");
      prep.addBatch();
      prep.clearBatch();

      prep.setString(1, "5");
      prep.addBatch();

      prep.executeBatch();
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM batchClear ORDER BY c0");
    Assertions.assertTrue(rs.next());
    assertEquals("1", rs.getString(1));
    Assertions.assertTrue(rs.next());
    assertEquals("2", rs.getString(1));
    Assertions.assertTrue(rs.next());
    assertEquals("3", rs.getString(1));
    Assertions.assertTrue(rs.next());
    assertEquals("5", rs.getString(1));
  }

  @Test
  public void batchError() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS t1");
    stmt.execute("CREATE TABLE t1(c0 DATE UNIQUE PRIMARY KEY NOT NULL)");

    stmt.addBatch("INSERT INTO t1 VALUES ('2006-04-01')");
    stmt.addBatch("INSERT INTO t1 VALUES ('2006-04-01')");
    stmt.addBatch("INSERT INTO t1 VALUES ('2019-04-11')");
    stmt.addBatch("INSERT INTO t1 VALUES ('2006-04-01')");
    stmt.addBatch("INSERT INTO t1 VALUES ('2020-04-11')");
    try {
      stmt.executeBatch();
      fail();
    } catch (BatchUpdateException e) {
      assertTrue(e.getMessage().contains("Duplicate entry"));
      assertEquals(5, e.getUpdateCounts().length);
      assertArrayEquals(
          new int[] {1, java.sql.Statement.EXECUTE_FAILED, 1, java.sql.Statement.EXECUTE_FAILED, 1},
          e.getUpdateCounts());
    }
  }

  @Test
  public void wrongParameter() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      wrongParameter(con);
    }
    try (Connection con = createCon("&useServerPrepStmts=true")) {
      wrongParameter(con);
    }
  }

  public void wrongParameter(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 5);
      try {
        prep.addBatch();
      } catch (SQLTransientConnectionException e) {
        assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
      }
      try {
        prep.addBatch();
      } catch (SQLTransientConnectionException e) {
        assertTrue(
            e.getMessage().contains("Parameter at position 2 is not set")
                || e.getMessage()
                    .contains(
                        "batch set of parameters differ from previous set. All parameters must be set"));
      }

      prep.setInt(1, 5);
      prep.setString(3, "wrong position");
      Common.assertThrowsContains(
          SQLTransientConnectionException.class,
          prep::addBatch,
          "Parameter at position 2 is not set");

      prep.setInt(1, 5);
      prep.setString(2, "ok");
      prep.addBatch();
      prep.setString(2, "without position 1");
      prep.addBatch();
    }
  }

  @Test
  public void differentParameterType() throws SQLException {
    for (int i = 0; i < 2; ++i) {
      boolean rewriteBatchedStatements = (i == 0);
      try (Connection con =
          createCon(
              String.format(
                  "&rewriteBatchedStatements=%s&useServerPrepStmts=false",
                  rewriteBatchedStatements))) {
        differentParameterType(con, rewriteBatchedStatements);
      }
      try (Connection con =
          createCon(
              String.format(
                  "&rewriteBatchedStatements=%s&useServerPrepStmts=false&allowLocalInfile",
                  rewriteBatchedStatements))) {
        differentParameterType(con, rewriteBatchedStatements);
      }
      try (Connection con =
          createCon(
              String.format(
                  "&rewriteBatchedStatements=%s&useServerPrepStmts&disablePipeline=true",
                  rewriteBatchedStatements))) {
        differentParameterType(
            con, false /* rewriteBatchedStatements is false since useServerPrepStmts is true*/);
      }
    }
  }

  public void differentParameterType(Connection con, boolean rewriteBatchedStatements)
      throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "1");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      prep.setInt(1, 3);
      prep.setNull(2, Types.INTEGER);
      prep.addBatch();
      int[] res = prep.executeBatch();
      if (rewriteBatchedStatements) {
        assertEquals(1, res.length);
        assertEquals(3, res[0]);
      } else {
        assertEquals(3, res.length);
        assertEquals(1, res[0]);
        assertEquals(1, res[1]);
        assertEquals(1, res[2]);
      }
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1, t2");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("2", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertNull(rs.getString(2));
    assertFalse(rs.next());
  }

  @Test
  public void largeBatchWithRewrite() throws SQLException {
    for (int i = 0; i < 8; i++) {
      boolean useServerPrepStmts = (i & 1) > 0;
      boolean useCompression = (i & 2) > 0;
      boolean rewriteBatchedStatements = (i & 4) > 0;

      try (Connection con =
          createCon(
              String.format(
                  "&useServerPrepStmts=%s&useCompression=%s&rewriteBatchedStatements=%s",
                  useServerPrepStmts, useCompression, rewriteBatchedStatements))) {
        largeBatchWithRewrite(con, rewriteBatchedStatements, useServerPrepStmts);
      }
    }
  }

  public void largeBatchWithRewrite(
      Connection con, boolean rewriteBatchedStatements, boolean useServerPrepStmts)
      throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "one");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setString(2, "two");
      prep.addBatch();
      long[] res = prep.executeLargeBatch();
      if (rewriteBatchedStatements && !useServerPrepStmts) {
        assertEquals(1, res.length);
      } else {
        assertEquals(2, res.length);
      }
    }

    ResultSet rs1 = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1");
    assertTrue(rs1.next());
    assertEquals(1, rs1.getInt(1));
    assertEquals("one", rs1.getString(2));
    assertTrue(rs1.next());
    assertEquals(2, rs1.getInt(1));
    assertEquals("two", rs1.getString(2));
    assertFalse(rs1.next());
    con.commit();

    try (PreparedStatement prep = con.prepareStatement("UPDATE BatchTest set t2=? where t1=?")) {
      prep.setString(1, "11");
      prep.setInt(2, 1);

      prep.addBatch();

      prep.setInt(1, 22);
      prep.setInt(2, 2);
      prep.addBatch();
      prep.executeLargeBatch();
    }

    ResultSet rs2 = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1");
    assertTrue(rs2.next());
    assertEquals(1, rs2.getInt(1));
    assertEquals("11", rs2.getString(2));
    assertTrue(rs2.next());
    assertEquals(2, rs2.getInt(1));
    assertEquals("22", rs2.getString(2));
    assertFalse(rs2.next());
    con.commit();
  }

  @Test
  public void batchWithError() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts=false&allowLocalInfile")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&allowLocalInfile")) {
      batchWithError(con);
    }
  }

  private void batchWithError(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepareError");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE prepareError(id int primary key, val varchar(10))");
    stmt.execute("INSERT INTO prepareError(id, val) values (1, 'val1')");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO prepareError(id, val) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "val3");
      prep.addBatch();
      Common.assertThrowsContains(
          BatchUpdateException.class,
          () -> prep.executeBatch(),
          "Duplicate entry '1' for key 'PRIMARY'");
    }
  }

  @Test
  public void testRewriteBatchedPipelineForAllowMultiQueries() throws SQLException {

    try (Connection con =
        createCon(
            "&useServerPrepStmts=false&rewriteBatchedStatements=true&allowMultiQueries=true")) {
      Statement stmt = con.createStatement();
      stmt.execute("DROP TABLE IF EXISTS BatchTest");
      stmt.execute(
          "CREATE TABLE BatchTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");

      String sql = "insert INTO BatchTest(t1, t2) VALUES (?,?)";
      assertTrue(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep =
          con.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)) {
        prep.setInt(1, 1);
        prep.setString(2, "entry-1");
        prep.addBatch();

        prep.setInt(1, 2);
        prep.setString(2, "entry-2");
        prep.addBatch();

        prep.setInt(1, 3);
        prep.setString(2, "entry-3");
        prep.addBatch();
        prep.executeLargeBatch();

        ResultSet rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }

      ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1, t2");

      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("entry-1", rs.getString(2));

      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("entry-2", rs.getString(2));

      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertEquals("entry-3", rs.getString(2));

      assertFalse(rs.next());
    }
  }

  @Test
  public void testInsertRegEx() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false&rewriteBatchedStatements=true")) {
      Statement stmt = con.createStatement();

      // Testcase-1 -  Having 'INSERT' in the query string with uppercase where 'INSERT' keyword is
      // used for 'INSERT' action

      stmt.execute("DROP TABLE IF EXISTS BatchTest");
      stmt.execute(
          "CREATE TABLE BatchTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");

      String sql = "INSERT INTO BatchTest(t1, t2) VALUES(1,'testcase-1')";
      assertTrue(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      stmt.execute(sql);

      ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1, t2");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("testcase-1", rs.getString(2));

      // Testcase-2 - Having 'INSERT' in the query string with lowercase where 'INSERT' keyword is
      // used for 'INSERT' action
      sql = "insert INTO BatchTest(t1, t2) VALUES (?,?)";
      assertTrue(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 2);
        prep.setString(2, "testcase-2");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs = stmt.executeQuery("SELECT * FROM BatchTest where t1=2");
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("testcase-2", rs.getString(2));

      // Testcase-3 - Having 'INSERT' in the query string where 'INSERT' keyword is used for
      // 'INSERT' action and query string is having comment into that
      sql = "Insert INTO BatchTest(t1, t2) /*Comment Section*/ VALUES (?,?)";
      assertTrue(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 3);
        prep.setString(2, "testcase-3");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs = stmt.executeQuery("SELECT * FROM BatchTest where t1=3");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertEquals("testcase-3", rs.getString(2));

      // Testcase-4 - Having 'INSERT' in the query string where 'INSERT' keyword is used for
      // 'INSERT' action and query string is having comment into that
      sql = "/*Comment Section */ inSERT INTO BatchTest(t1, t2) VALUES (?,?)";
      assertTrue(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 4);
        prep.setString(2, "testcase-4");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs = stmt.executeQuery("SELECT * FROM BatchTest where t1=4");
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));
      assertEquals("testcase-4", rs.getString(2));

      // Testcase-5 - Having 'INSERT' in the query string where 'INSERT' keyword is used for
      // 'INSERT' action and query string is having comment into that
      sql = "inSERT INTO BatchTest(t1, t2) VALUES (?,?) /*Comment Section */ ";
      assertTrue(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 5);
        prep.setString(2, "testcase-5");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs = stmt.executeQuery("SELECT * FROM BatchTest where t1=5");
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals("testcase-5", rs.getString(2));

      // Testcase-6 - Not Having 'INSERT' keyword in the query string
      sql = "Select * from BatchTest where t1=5";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());
      rs = stmt.executeQuery(sql);
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals("testcase-5", rs.getString(2));

      // Testcase-7 - Table name and Column name can not be 'insert'. So testing a scenario where
      // table name and column name are having 'insert' in their names and value of one
      // of the column is 'insert'
      stmt.execute("DROP TABLE IF EXISTS `insert`");
      stmt.execute(
          "CREATE TABLE `insert` (`insert` int not null primary key auto_increment, insertTest LONGTEXT, INSERTTEST1 LONGTEXT)");

      sql = "insert INTO `insert`(`insert`, insertTest, INSERTTEST1) VALUES (?,?, ?)";
      assertTrue(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 7);
        prep.setString(2, "insert");
        prep.setString(3, "INSERT");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      sql = "Select * from `insert`";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());
      rs = stmt.executeQuery(sql);
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertEquals("insert", rs.getString(2));
      assertEquals("INSERT", rs.getString(3));

      // Testcase-8 - Update Query having 'insert' as a value in the 'where' clause
      sql = "update `insert` set insertTest=? where INSERTTEST1=?";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setString(1, "testcase-8");
        prep.setString(2, "INSERT");

        prep.addBatch();
        prep.executeLargeBatch();
      }

      sql = "Select * from `insert`";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());
      rs = stmt.executeQuery(sql);
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertEquals("testcase-8", rs.getString(2));
      assertEquals("INSERT", rs.getString(3));

      stmt.execute("DROP TABLE IF EXISTS `insert`");

      // Testcase-9 - update query without 'insert' keyword
      sql = "update BatchTest set t2=? where t1=1";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setString(1, "testcase-9");
        prep.setInt(2, 1);

        prep.addBatch();
        prep.executeLargeBatch();
      }

      sql = "Select * from BatchTest where t1=1";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());
      rs = stmt.executeQuery(sql);
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("testcase-9", rs.getString(2));

      // Testcase-10 -  Delete query without 'insert' keyword
      sql = "delete from BatchTest where t1=1";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());
      stmt.execute(sql);

      sql = "Select * from BatchTest where t1=1";
      assertFalse(ClientParser.INSERT_STATEMENT_PATTERN.matcher(sql).find());
      rs = stmt.executeQuery(sql);
      assertFalse(rs.next());
    }
  }

  @Test
  public void testRewriteBatchedPipelineForInsertOnDuplicateKeyUpdate() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false&rewriteBatchedStatements=true")) {
      Statement stmt = con.createStatement();

      // Testcase-1 -  Having 'ON DUPLICATE KEY UPDATE' clause in the query string
      stmt.execute("DROP TABLE IF EXISTS BatchTest");
      stmt.execute(
          "CREATE ROWSTORE TABLE BatchTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");

      String sql = "INSERT INTO BatchTest(t1, t2) VALUES (?,?) ON DUPLICATE KEY UPDATE t1=t1+1";
      assertTrue(ClientParser.INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN.matcher(sql).find());
      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 1);
        prep.setString(2, "testcase-1");
        prep.addBatch();

        prep.executeLargeBatch();
      }

      ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1, t2");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("testcase-1", rs.getString(2));
      stmt.execute("DROP TABLE IF EXISTS BatchTest");

      // Testcase-2-  Having 'ON DUPLICATE KEY UPDATE' as a clause and as a table name in the query
      // string
      stmt.execute("DROP TABLE IF EXISTS `ON DUPLICATE KEY UPDATE`");
      stmt.execute(
          "CREATE ROWSTORE TABLE `ON DUPLICATE KEY UPDATE` (t1 int not null primary key auto_increment, t2 LONGTEXT)");

      sql =
          "INSERT INTO `ON DUPLICATE KEY UPDATE` (`t1`, `t2`) VALUES (?, ?) ON DUPLICATE       KEY UPDATE `t1`=t1+1";
      assertTrue(ClientParser.INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 2);
        prep.setString(2, "testcase-2");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs = stmt.executeQuery("SELECT * FROM `ON DUPLICATE KEY UPDATE` where t1=2");
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("testcase-2", rs.getString(2));

      // Testcase-3-  Having 'ON DUPLICATE KEY UPDATE' as a clause and as a table name in the query
      // string. Plus having a comment in between the 'ON DUPLICATE KEY UPDATE' clause
      sql =
          "INSERT INTO `ON DUPLICATE KEY UPDATE` (`t1`, `t2`) VALUES (?, ?) ON DUPLICATE  /*Comment Section*/ KEY UPDATE t1=t1+1";
      assertTrue(ClientParser.INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 3);
        prep.setString(2, "testcase-3");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs = stmt.executeQuery("SELECT * FROM `ON DUPLICATE KEY UPDATE` where t1=3");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertEquals("testcase-3", rs.getString(2));

      // Testcase-4-  Having 'ON DUPLICATE KEY UPDATE' as a table name in the query string
      sql = "INSERT INTO `ON DUPLICATE KEY UPDATE` (`t1`, `t2`) VALUES (?, ?)";
      assertFalse(
          ClientParser.INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN.matcher(sql).find());
      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 4);
        prep.setString(2, "testcase-4");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs = stmt.executeQuery("SELECT * FROM `ON DUPLICATE KEY UPDATE` where t1=4");
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));
      assertEquals("testcase-4", rs.getString(2));

      // Testcase-5-  Having 'ON DUPLICATE KEY UPDATE' as a clause, as a table name and as a Column
      // Name in the query string. Plus having a comment in between the 'ON DUPLICATE KEY UPDATE'
      // clause
      stmt.execute("DROP TABLE IF EXISTS `ON DUPLICATE KEY UPDATE`");
      stmt.execute(
          "CREATE TABLE `ON DUPLICATE KEY UPDATE` (`ON DUPLICATE KEY UPDATE` int not null primary key auto_increment, t2 LONGTEXT)");

      sql =
          "INSERT INTO `ON DUPLICATE KEY UPDATE` (`ON DUPLICATE KEY UPDATE`, `t2`) VALUES (?, ?) ON DUPLICATE  /*Comment Section*/ KEY UPDATE t2='dummy'";
      assertTrue(ClientParser.INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN.matcher(sql).find());

      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 5);
        prep.setString(2, "testcase-5");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs =
          stmt.executeQuery(
              "SELECT * FROM `ON DUPLICATE KEY UPDATE` where `ON DUPLICATE KEY UPDATE`=5");
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals("testcase-5", rs.getString(2));

      // Testcase-6-  Having 'ON DUPLICATE KEY UPDATE' as a clause, as a table name and as a Column
      // Name in the query string. Plus having a comment in between the 'ON DUPLICATE KEY UPDATE'
      // clause
      sql = "INSERT INTO `ON DUPLICATE KEY UPDATE` (`ON DUPLICATE KEY UPDATE`, `t2`) VALUES (?, ?)";
      assertFalse(
          ClientParser.INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN.matcher(sql).find());
      try (PreparedStatement prep = con.prepareStatement(sql)) {
        prep.setInt(1, 6);
        prep.setString(2, "testcase-6");
        prep.addBatch();
        prep.executeLargeBatch();
      }

      rs =
          stmt.executeQuery(
              "SELECT * FROM `ON DUPLICATE KEY UPDATE` where `ON DUPLICATE KEY UPDATE`=6");
      assertTrue(rs.next());
      assertEquals(6, rs.getInt(1));
      assertEquals("testcase-6", rs.getString(2));
      stmt.execute("DROP TABLE IF EXISTS `ON DUPLICATE KEY UPDATE`");
    }
  }
}
