/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiTest extends BaseTest {

  /** Tables initialisation. */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("MultiTestt1", "id int, test varchar(100)");
    createTable("MultiTestt2", "id int, test varchar(100)");
    createTable("MultiTestt3", "message text");
    createTable("MultiTestt4", "id int, test varchar(100), PRIMARY KEY (`id`)");
    createTable("MultiTestt5", "id int, test varchar(100)");
    createTable("MultiTestt6", "id int, test varchar(100)");
    createTable("MultiTestt7", "id int, test varchar(100)");
    createTable("MultiTestt8", "id int, test varchar(100)");
    createTable("MultiTestt10", "id int");
    createTable(
        "MultiTestreWriteDuplicateTestTable", "id int, name varchar(100), PRIMARY KEY (`id`)");
    createTable("MultiTesttselect1", "LAST_UPDATE_DATETIME TIMESTAMP , nn int");
    createTable("MultiTesttselect2", "nn int");
    createTable("MultiTesttselect3", "LAST_UPDATE_DATETIME TIMESTAMP , nn int");
    createTable("MultiTesttselect4", "nn int");
    createTable(
        "MultiTestt3_dupp",
        "col1 int, pkey int NOT NULL, col2 int, col3 int, col4 int, PRIMARY KEY " + "(`pkey`)");
    createTable(
        "MultiTesttest_table",
        "col1 VARCHAR(32), col2 VARCHAR(32), col3 VARCHAR(32), col4 VARCHAR(32), "
            + "col5 VARCHAR(32)");
    createTable(
        "MultiTesttest_table2",
        "col1 VARCHAR(32), col2 VARCHAR(32), col3 VARCHAR(32), col4 VARCHAR(32), "
            + "col5 VARCHAR(32)");
    createTable("MultiTestValues", "col1 VARCHAR(32), col2 VARCHAR(32)");

    createTable("MultiTestprepsemi", "id int not null primary key auto_increment, text text");
    createTable("MultiTestA", "data varchar(10)");
    createTable("testMultiGeneratedKey", "id int not null primary key auto_increment, text text");

    if (testSingleHost) {
      Statement st = sharedConnection.createStatement();
      st.execute("insert into MultiTestt1 values(1,'a'),(2,'a')");
      st.execute("insert into MultiTestt2 values(1,'a'),(2,'a')");
      st.execute("insert into MultiTestt5 values(1,'a'),(2,'a'),(2,'b')");
    }
  }

  @Test
  public void rewriteSelectQuery() throws Throwable {
    Statement st = sharedConnection.createStatement();
    st.execute("INSERT INTO MultiTesttselect2 VALUES (1)");
    PreparedStatement ps =
        sharedConnection.prepareStatement(
            "/*CLIENT*/ insert into MultiTesttselect1 "
                + "(LAST_UPDATE_DATETIME, nn) select ?, nn from MultiTesttselect2");
    ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
    ps.executeUpdate();

    ResultSet rs = st.executeQuery("SELECT * FROM MultiTesttselect1");
    assertTrue(rs.next());
    assertEquals(rs.getInt(2), 1);
  }

  @Test
  public void rewriteParsingDoubleSlash() throws Throwable {
    parsingDoubleSlash(sharedConnection);

    try (Connection conn = setConnection("&rewriteBatchedStatements=true")) {
      parsingDoubleSlash(conn);

      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE rewriteSlash(t varchar(50))");
      stmt.addBatch("insert into rewriteSlash values ('\\\\')");
      stmt.executeBatch();

      ResultSet rs = stmt.executeQuery("SELECT * FROM rewriteSlash");
      assertTrue(rs.next());
      assertEquals(rs.getString(1), "\\");
    }
  }

  private void parsingDoubleSlash(Connection conn) throws SQLException {
    try (PreparedStatement p = conn.prepareStatement("SELECT '\\\\', ?")) {
      p.setString(1, "\\\\");
      ResultSet rs = p.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString(1), "\\");
      assertEquals(rs.getString(2), "\\\\");
    }
  }

  @Test
  public void rewriteSelectQueryServerPrepared() throws Throwable {
    Statement st = sharedConnection.createStatement();
    st.execute("INSERT INTO MultiTesttselect4 VALUES (1)");
    PreparedStatement ps =
        sharedConnection.prepareStatement(
            "insert into MultiTesttselect3 (LAST_UPDATE_DATETIME,"
                + " nn) select ?, nn from MultiTesttselect4");
    ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
    ps.executeUpdate();

    ResultSet rs = st.executeQuery("SELECT * FROM MultiTesttselect3");
    assertTrue(rs.next());
    assertEquals(rs.getInt(2), 1);
  }

  @Test
  public void basicTest() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery("select * from MultiTestt1;select * from MultiTestt2;");
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertTrue(count > 0);
      assertTrue(statement.getMoreResults());
      rs = statement.getResultSet();
      count = 0;
      while (rs.next()) {
        count++;
      }
      assertTrue(count > 0);
      assertFalse(statement.getMoreResults());
    }
  }

  @Test
  public void updateTest() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      Statement statement = connection.createStatement();
      statement.execute(
          "update MultiTestt5 set test='a "
              + System.currentTimeMillis()
              + "' where id = 2;select * from MultiTestt2;update MultiTestt5 set test='a2 "
              + System.currentTimeMillis()
              + "' where id = 1;");
      assertNull(statement.getResultSet());
      assertEquals(2, statement.getUpdateCount());
      assertTrue(statement.getMoreResults());
      assertEquals(-1, statement.getUpdateCount());
      ResultSet rs = statement.getResultSet();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertTrue(count > 0);

      assertFalse(statement.getMoreResults());
      assertEquals(1, statement.getUpdateCount());
      assertNull(statement.getResultSet());
      assertFalse(statement.getMoreResults());
      assertEquals(-1, statement.getUpdateCount());
    }
  }

  @Test
  public void updateTest2() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      Statement statement = connection.createStatement();
      statement.execute(
          "select * from MultiTestt2;update MultiTestt5 set test='a "
              + System.currentTimeMillis()
              + "' where id = 2;");
      ResultSet rs = statement.getResultSet();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertTrue(count == 2);
      statement.getMoreResults();

      int updateNb = statement.getUpdateCount();
      assertEquals(2, updateNb);
    }
  }

  @Test
  public void selectTest() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      Statement statement = connection.createStatement();
      statement.execute("select * from MultiTestt2;select * from MultiTestt1;");
      ResultSet rs = statement.getResultSet();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertTrue(count > 0);
      rs = statement.executeQuery("select * from MultiTestt1");
      count = 0;
      while (rs.next()) {
        count++;
      }
      assertTrue(count > 0);
    }
  }

  @Test
  public void setMaxRowsMulti() throws Exception {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      Statement st = connection.createStatement();
      assertEquals(0, st.getMaxRows());

      st.setMaxRows(1);
      assertEquals(1, st.getMaxRows());

      /* Check 3 rows are returned if maxRows is limited to 3, in every result set in batch */

      /* Check first result set for at most 3 rows*/
      ResultSet rs = st.executeQuery("select 1 union select 2;select 1 union select 2");
      int cnt = 0;

      while (rs.next()) {
        cnt++;
      }
      rs.close();
      assertEquals(1, cnt);

      /* Check second result set for at most 3 rows*/
      assertTrue(st.getMoreResults());
      rs = st.getResultSet();
      cnt = 0;
      while (rs.next()) {
        cnt++;
      }
      rs.close();
      assertEquals(1, cnt);
    }
  }

  /**
   * Conj-99: rewriteBatchedStatements parameter.
   *
   * @throws SQLException exception
   */
  @Test
  public void rewriteBatchedStatementsDisabledInsertionTest() throws SQLException {
    verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.FALSE, 3000);
  }

  /**
   * Conj-206: rewriteBatchedStatements parameter take care of max_allowed_size.
   *
   * @throws SQLException exception
   */
  @Test
  public void rewriteBatchedMaxAllowedSizeTest() throws SQLException {

    createTable("MultiTestt6", "id int, test varchar(10000)");
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("rewriteBatchedMaxAllowedSizeTest"));
    Statement st = sharedConnection.createStatement();
    ResultSet rs = st.executeQuery("select @@max_allowed_packet");
    if (rs.next()) {
      long maxAllowedPacket = rs.getInt(1);
      Assume.assumeTrue(maxAllowedPacket < 512 * 1024 * 1024L);
      int totalInsertCommands = (int) Math.ceil(maxAllowedPacket / 10050);
      verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.TRUE, totalInsertCommands);
    } else {
      fail();
    }
  }

  @Test
  public void rewriteBatchedWithoutParam() throws SQLException {
    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO MultiTestt10 VALUES (1)");
      for (int i = 0; i < 100; i++) {
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
      ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM MultiTestt10");
      assertTrue(rs.next());
      assertEquals(100, rs.getInt(1));
    }
  }

  /**
   * CONJ-329 error for rewrite without parameter.
   *
   * @throws SQLException if exception occur
   */
  @Test
  public void rewriteStatementWithoutParameter() throws SQLException {
    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
        statement.executeQuery();
      }
    }
  }

  /**
   * CONJ-330 - correction using execute...() for rewriteBatchedStatements
   *
   * @throws SQLException if exception occur
   */
  @Test
  public void rewriteMonoQueryStatementWithParameter() throws SQLException {
    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      String failingQuery1 = "SELECT (1=? AND 2=2)";
      String failingQuery2 = "SELECT (1=?) AND 2=2";
      String workingQuery = "SELECT 1=? AND (2=2)";

      try (PreparedStatement statement = connection.prepareStatement(failingQuery1)) {
        checkResult(statement);
      }

      try (PreparedStatement statement = connection.prepareStatement(failingQuery2)) {
        checkResult(statement);
      }

      try (PreparedStatement statement = connection.prepareStatement(workingQuery)) {
        checkResult(statement);
      }
    }
  }

  private void checkResult(PreparedStatement statement) throws SQLException {
    statement.setInt(1, 1);
    statement.executeQuery();
    ResultSet rs = statement.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
  }

  @Test
  public void testServerPrepareMeta() throws Throwable {
    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      createTable("insertSelectTable1", "tt int");
      createTable("insertSelectTable2", "tt int");
      Statement stmt = connection.createStatement();
      stmt.execute("INSERT INTO insertSelectTable2(tt) VALUES (1),(2),(1)");

      PreparedStatement ps =
          connection.prepareStatement(
              "INSERT INTO insertSelectTable1 "
                  + "SELECT a1.tt FROM insertSelectTable2 a1 "
                  + "WHERE a1.tt = ? ");
      ps.setInt(1, 1);
      ps.addBatch();
      ps.setInt(1, 2);
      ps.addBatch();
      ps.executeBatch();

      ResultSet rs = stmt.executeQuery("SELECT count(*) FROM insertSelectTable1");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
    }
  }

  private void verifyInsertBehaviorBasedOnRewriteBatchedStatements(
      Boolean rewriteBatchedStatements, int totalInsertCommands) throws SQLException {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", rewriteBatchedStatements.toString());
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      verifyInsertCount(tmpConnection, 0);
      Statement statement = tmpConnection.createStatement();
      for (int i = 0; i < totalInsertCommands; i++) {
        statement.addBatch("INSERT INTO MultiTestt6 VALUES (" + i + ", 'testValue" + i + "')");
      }
      int[] updateCounts = statement.executeBatch();
      assertEquals(totalInsertCommands, updateCounts.length);
      int totalUpdates = 0;
      for (int updateCount : updateCounts) {
        assertEquals(1, updateCount);
        totalUpdates += updateCount;
      }
      assertEquals(totalInsertCommands, totalUpdates);
      verifyInsertCount(tmpConnection, totalInsertCommands);
    }
  }

  private void verifyInsertCount(Connection tmpConnection, int insertCount) throws SQLException {
    assertEquals(insertCount, retrieveSessionVariableFromServer(tmpConnection, "Com_insert"));
  }

  private int retrieveSessionVariableFromServer(Connection tmpConnection, String variable)
      throws SQLException {
    Statement statement = tmpConnection.createStatement();
    try (ResultSet resultSet = statement.executeQuery("SHOW STATUS LIKE '" + variable + "'")) {
      if (resultSet.next()) {
        return resultSet.getInt(2);
      }
    }
    throw new SQLException("Unable to retrieve, variable value from Server " + variable);
  }

  /**
   * Conj-141 : Batch Statement Rewrite: Support for ON DUPLICATE KEY.
   *
   * @throws SQLException exception
   */
  @Test
  public void rewriteBatchedStatementsWithQueryFirstAndLAst() throws SQLException {
    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      PreparedStatement sqlInsert =
          connection.prepareStatement(
              "INSERT INTO MultiTestt3_dupp(col1, pkey,col2,"
                  + "col3,col4) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE pkey=pkey+10");
      sqlInsert.setInt(1, 1);
      sqlInsert.setInt(2, 2);
      sqlInsert.addBatch();

      sqlInsert.setInt(1, 2);
      sqlInsert.setInt(2, 5);
      sqlInsert.addBatch();

      sqlInsert.setInt(1, 7);
      sqlInsert.setInt(2, 6);
      sqlInsert.addBatch();
      sqlInsert.executeBatch();
    }
  }

  /**
   * Conj-142: Using a semicolon in a string with "rewriteBatchedStatements=true" fails.
   *
   * @throws SQLException exception
   */
  @Test
  public void rewriteBatchedStatementsSemicolon() throws SQLException {
    // set the rewrite batch statements parameter
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");

    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      final int currentInsert = retrieveSessionVariableFromServer(tmpConnection, "Com_insert");

      PreparedStatement sqlInsert =
          tmpConnection.prepareStatement("INSERT INTO MultiTestt3 (message) VALUES (?)");
      sqlInsert.setString(1, "aa");
      sqlInsert.addBatch();
      sqlInsert.setString(1, "b;b");
      sqlInsert.addBatch();
      sqlInsert.setString(1, ";ccccccc");
      sqlInsert.addBatch();
      sqlInsert.setString(1, "ddddddddddddddd;");
      sqlInsert.addBatch();
      sqlInsert.setString(1, ";eeeeeee;;eeeeeeeeee;eeeeeeeeee;");
      sqlInsert.addBatch();
      int[] updateCounts = sqlInsert.executeBatch();

      // rewrite should be ok, so the above should be executed in 1 command updating 5 rows
      assertEquals(5, updateCounts.length);
      assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[0]);
      assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[1]);
      assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[2]);
      assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[3]);
      assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[4]);
      assertEquals(
          1, retrieveSessionVariableFromServer(tmpConnection, "Com_insert") - currentInsert);

      int[] realUpdateCount = ((ClientSidePreparedStatement) sqlInsert).getServerUpdateCounts();
      assertEquals(1, realUpdateCount.length);
      assertEquals(5, realUpdateCount[0]);

      final int secondCurrentInsert =
          retrieveSessionVariableFromServer(tmpConnection, "Com_insert");

      // rewrite for multiple statements isn't possible, so use allowMutipleQueries
      sqlInsert =
          tmpConnection.prepareStatement(
              "INSERT INTO MultiTestt3 (message) VALUES (?); "
                  + "INSERT INTO MultiTestt3 (message) VALUES ('multiple')");
      sqlInsert.setString(1, "aa");
      sqlInsert.addBatch();
      sqlInsert.setString(1, "b;b");
      sqlInsert.addBatch();
      try {
        updateCounts = sqlInsert.executeBatch();
        assertEquals(4, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
        assertEquals(1, updateCounts[2]);
        assertEquals(1, updateCounts[3]);

        assertEquals(
            4,
            retrieveSessionVariableFromServer(tmpConnection, "Com_insert") - secondCurrentInsert);
      } catch (BatchUpdateException bue) {
        if ((sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2))) {
          assertTrue(bue.getMessage().contains("You have an error in your SQL syntax"));
        } else {
          fail(bue.getMessage());
        }
      }
    }
  }

  private PreparedStatement prepareStatementBatch(Connection tmpConnection, int size)
      throws SQLException {
    PreparedStatement preparedStatement =
        tmpConnection.prepareStatement("INSERT INTO MultiTestt7 VALUES (?, ?)");
    for (int i = 0; i < size; i++) {
      preparedStatement.setInt(1, i);
      preparedStatement.setString(2, "testValue" + i);
      preparedStatement.addBatch();

      preparedStatement.setInt(1, i);
      preparedStatement.setString(2, "testSecn" + i);
      preparedStatement.addBatch();
    }
    return preparedStatement;
  }

  /**
   * Conj-215: Batched statements with rewriteBatchedStatements that end with a semicolon fails.
   *
   * @throws SQLException exception
   */
  @Test
  public void semicolonTest() throws SQLException {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      Statement sqlInsert = tmpConnection.createStatement();
      verifyInsertCount(tmpConnection, 0);

      for (int i = 0; i < 100; i++) {
        sqlInsert.addBatch(
            "insert into MultiTestprepsemi (text) values ('This is a test" + i + "');");
      }
      sqlInsert.executeBatch();
      verifyInsertCount(tmpConnection, 100);
      for (int i = 0; i < 100; i++) {
        sqlInsert.addBatch(
            "insert into MultiTestprepsemi (text) values ('This is a test" + i + "')");
      }
      sqlInsert.executeBatch();
      verifyInsertCount(tmpConnection, 200);
    }
  }

  /**
   * Conj-99: rewriteBatchedStatements parameter.
   *
   * @throws SQLException exception
   */
  @Test
  public void rewriteBatchedStatementsUpdateTest() throws SQLException {
    // set the rewrite batch statements parameter
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      tmpConnection.setClientInfo(props);
      verifyUpdateCount(tmpConnection, 0);
      int cycles = 1000;
      prepareStatementBatch(tmpConnection, cycles).executeBatch(); // populate the table
      PreparedStatement preparedStatement =
          tmpConnection.prepareStatement("UPDATE MultiTestt7 SET test = ? WHERE id = ?");
      for (int i = 0; i < cycles; i++) {
        preparedStatement.setString(1, "updated testValue" + i);
        preparedStatement.setInt(2, i);
        preparedStatement.addBatch();
      }
      int[] updateCounts = preparedStatement.executeBatch();
      assertEquals(cycles, updateCounts.length);
      for (int updateCount : updateCounts) {
        assertTrue(
            updateCount == 2
                || updateCount == Statement.SUCCESS_NO_INFO); // 2 rows updated by update.
      }

      verifyUpdateCount(tmpConnection, cycles); // 1000 update commande launched
    }
  }

  /**
   * Conj-152: rewriteBatchedStatements and multiple executeBatch check.
   *
   * @throws SQLException exception
   */
  @Test
  public void testMultipleExecuteBatch() throws SQLException {
    // set the rewrite batch statements parameter
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      tmpConnection.setClientInfo(props);
      verifyUpdateCount(tmpConnection, 0);
      tmpConnection.createStatement().execute("insert into MultiTestt8 values(1,'a'),(2,'a')");

      PreparedStatement preparedStatement =
          tmpConnection.prepareStatement("UPDATE MultiTestt8 SET test = ? WHERE id = ?");
      preparedStatement.setString(1, "executebatch");
      preparedStatement.setInt(2, 1);
      preparedStatement.addBatch();
      preparedStatement.setString(1, "executebatch2");
      preparedStatement.setInt(2, 3);
      preparedStatement.addBatch();

      int[] updateCounts = preparedStatement.executeBatch();
      assertEquals(2, updateCounts.length);

      preparedStatement.setString(1, "executebatch3");
      preparedStatement.setInt(2, 1);
      preparedStatement.addBatch();
      updateCounts = preparedStatement.executeBatch();
      assertEquals(1, updateCounts.length);
    }
  }

  @Test
  public void rewriteBatchedStatementsInsertWithDuplicateRecordsTest() throws SQLException {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      verifyInsertCount(tmpConnection, 0);
      Statement statement = tmpConnection.createStatement();
      for (int i = 0; i < 100; i++) {
        int newId = i % 20; // to create duplicate id's
        String roleTxt = "VAMPIRE" + newId;
        statement.addBatch(
            "INSERT IGNORE  INTO MultiTestreWriteDuplicateTestTable VALUES ("
                + newId
                + ", '"
                + roleTxt
                + "')");
      }
      int[] updateCounts = statement.executeBatch();
      assertEquals(100, updateCounts.length);

      for (int i = 0; i < updateCounts.length; i++) {
        assertEquals((i < 20) ? 1 : 0, updateCounts[i]);
      }
      verifyInsertCount(tmpConnection, 100);
      verifyUpdateCount(tmpConnection, 0);
    }
  }

  @Test
  public void updateCountTest() throws SQLException {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    props.setProperty("useBulkStmts", "false");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      PreparedStatement sqlInsert =
          tmpConnection.prepareStatement("INSERT IGNORE INTO MultiTestt4 (id,test) VALUES (?,?)");
      sqlInsert.setInt(1, 1);
      sqlInsert.setString(2, "value1");
      sqlInsert.addBatch();
      sqlInsert.setInt(1, 1);
      sqlInsert.setString(2, "valuenull");
      sqlInsert.addBatch();
      sqlInsert.setInt(1, 2);
      sqlInsert.setString(2, "value2");
      sqlInsert.addBatch();
      sqlInsert.setInt(1, 3);
      sqlInsert.setString(2, "value2");
      sqlInsert.addBatch();
      int[] insertCounts = sqlInsert.executeBatch();

      // Insert in prepare statement, cannot know the number og each one
      assertEquals(4, insertCounts.length);
      assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[0]);
      assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[1]);
      assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[2]);
      assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[3]);

      PreparedStatement sqlUpdate =
          tmpConnection.prepareStatement("UPDATE MultiTestt4 SET test = ? WHERE test = ?");
      sqlUpdate.setString(1, "value1 - updated");
      sqlUpdate.setString(2, "value1");
      sqlUpdate.addBatch();
      sqlUpdate.setString(1, "value3 - updated");
      sqlUpdate.setString(2, "value3");
      sqlUpdate.addBatch();
      sqlUpdate.setString(1, "value2 - updated");
      sqlUpdate.setString(2, "value2");
      sqlUpdate.addBatch();

      int[] updateCounts = sqlUpdate.executeBatch();
      assertEquals(3, updateCounts.length);
      assertEquals(1, updateCounts[0]);
      assertEquals(0, updateCounts[1]);
      assertEquals(2, updateCounts[2]);
    }
  }

  private void verifyUpdateCount(Connection tmpConnection, int updateCount) throws SQLException {
    assertEquals(updateCount, retrieveSessionVariableFromServer(tmpConnection, "Com_update"));
  }

  @Test
  public void testInsertWithLeadingConstantValue() throws Exception {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      PreparedStatement insertStmt =
          tmpConnection.prepareStatement(
              "INSERT INTO MultiTesttest_table (col1, col2,"
                  + " col3, col4, col5) values('some value', ?, 'other value', ?, 'third value')");
      insertStmt.setString(1, "a1");
      insertStmt.setString(2, "a2");
      insertStmt.addBatch();
      insertStmt.setString(1, "b1");
      insertStmt.setString(2, "b2");
      insertStmt.addBatch();
      insertStmt.executeBatch();
    }
  }

  @Test
  public void testInsertWithoutFirstContent() throws Exception {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      PreparedStatement insertStmt =
          tmpConnection.prepareStatement(
              "INSERT INTO MultiTesttest_table2 "
                  + "(col2, col3, col4, col5) values(?, 'other value', ?, 'third value')");
      insertStmt.setString(1, "a1");
      insertStmt.setString(2, "a2");
      insertStmt.addBatch();
      insertStmt.setString(1, "b1");
      insertStmt.setString(2, "b2");
      insertStmt.addBatch();
      insertStmt.executeBatch();
    }
  }

  @Test
  public void testduplicate() throws Exception {
    createTable(
        "SOME_TABLE",
        "ID INT(11) not null, FOO INT(11), PRIMARY KEY (ID), UNIQUE INDEX `FOO` (`FOO`)");
    String sql =
        "insert into `SOME_TABLE` (`ID`, `FOO`) values (?, ?) "
            + "on duplicate key update `SOME_TABLE`.`FOO` = ?";
    PreparedStatement st = sharedConnection.prepareStatement(sql);
    st.setInt(1, 1);
    st.setInt(2, 1);
    st.setInt(3, 1);
    st.addBatch();

    st.setInt(1, 2);
    st.setInt(2, 1);
    st.setInt(3, 2);
    st.addBatch();
    st.executeBatch();

    sql = "/*CLIENT*/" + sql;
    st = sharedConnection.prepareStatement(sql);
    st.setInt(1, 4);
    st.setInt(2, 4);
    st.setInt(3, 5);
    st.addBatch();

    st.setInt(1, 5);
    st.setInt(2, 4);
    st.setInt(3, 8);
    st.addBatch();
    st.executeBatch();
  }

  @Test
  public void valuesWithoutSpace() throws Exception {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      PreparedStatement insertStmt =
          tmpConnection.prepareStatement("INSERT INTO MultiTestValues (col1, col2)VALUES (?, ?)");
      insertStmt.setString(1, "a");
      insertStmt.setString(2, "b");
      insertStmt.addBatch();
      insertStmt.setString(1, "c");
      insertStmt.setString(2, "d");
      insertStmt.addBatch();
      insertStmt.executeBatch();
    }
  }

  /**
   * Conj-208 : Rewritten batch inserts can fail without a space before the VALUES clause.
   *
   * @throws Exception exception
   */
  @Test
  public void valuesWithoutSpacewithoutRewrite() throws Exception {
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      PreparedStatement insertStmt =
          tmpConnection.prepareStatement("INSERT INTO MultiTestValues (col1, col2)VALUES (?, ?)");
      insertStmt.setString(1, "a");
      insertStmt.setString(2, "b");
      insertStmt.addBatch();
      insertStmt.setString(1, "c");
      insertStmt.setString(2, "d");
      insertStmt.addBatch();
      insertStmt.executeBatch();
    }
  }

  @Test
  public void continueOnBatchError() throws SQLException {
    for (int i = 0; i < 16; i++) {
      continueOnBatchError(i % 16 < 8, i % 8 < 4, i % 4 < 2, i % 2 == 0);
    }
  }

  private void continueOnBatchError(
      boolean continueBatch, boolean serverPrepare, boolean rewrite, boolean batchMulti)
      throws SQLException {
    System.out.println(
        "continueBatch:"
            + continueBatch
            + " serverPrepare:"
            + serverPrepare
            + " rewrite:"
            + rewrite
            + " batchMulti:"
            + batchMulti);
    createTable("MultiTestt9", "id int not null primary key, test varchar(10)");
    Assume.assumeTrue(!batchMulti || !sharedIsAurora());

    try (Connection connection =
        setBlankConnection(
            "&useServerPrepStmts="
                + serverPrepare
                + "&useBatchMultiSend="
                + batchMulti
                + "&continueBatchOnError="
                + continueBatch
                + "&rewriteBatchedStatements="
                + rewrite)) {
      PreparedStatement pstmt =
          connection.prepareStatement("INSERT INTO MultiTestt9 (id, test) VALUES (?, ?)");
      for (int i = 0; i < 10; i++) {
        pstmt.setInt(1, (i == 5) ? 0 : i);
        pstmt.setString(2, String.valueOf(i));
        pstmt.addBatch();
      }
      try {
        pstmt.executeBatch();
        fail("Must have thrown SQLException");
      } catch (BatchUpdateException e) {

        int[] updateCount = e.getUpdateCounts();
        assertEquals(10, updateCount.length);
        if (rewrite) {
          // rewrite exception is all or nothing
          assertEquals(Statement.EXECUTE_FAILED, updateCount[0]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[1]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[2]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[3]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[4]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[5]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[6]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[7]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[8]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[9]);
        } else {
          assertEquals(1, updateCount[0]);
          assertEquals(1, updateCount[1]);
          assertEquals(1, updateCount[2]);
          assertEquals(1, updateCount[3]);
          assertEquals(1, updateCount[4]);
          assertEquals(Statement.EXECUTE_FAILED, updateCount[5]);
          if (continueBatch) {
            assertEquals(1, updateCount[6]);
            assertEquals(1, updateCount[7]);
            assertEquals(1, updateCount[8]);
            assertEquals(1, updateCount[9]);
          } else {
            if (batchMulti) {
              // send in batch, so continue will be handle, but send packet is executed.
              assertEquals(1, updateCount[6]);
              assertEquals(1, updateCount[7]);
              assertEquals(1, updateCount[8]);
              assertEquals(1, updateCount[9]);
            } else {
              assertEquals(Statement.EXECUTE_FAILED, updateCount[6]);
              assertEquals(Statement.EXECUTE_FAILED, updateCount[7]);
              assertEquals(Statement.EXECUTE_FAILED, updateCount[8]);
              assertEquals(Statement.EXECUTE_FAILED, updateCount[9]);
            }
          }
        }

        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM MultiTestt9");
        // check result
        if (!rewrite) {
          checkNextData(0, rs);
          checkNextData(1, rs);
          checkNextData(2, rs);
          checkNextData(3, rs);
          checkNextData(4, rs);

          if (continueBatch || batchMulti) {
            checkNextData(6, rs);
            checkNextData(7, rs);
            checkNextData(8, rs);
            checkNextData(9, rs);
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  private void checkNextData(int value, ResultSet rs) throws SQLException {
    assertTrue(rs.next());
    assertEquals(value, rs.getInt(1));
    assertEquals(String.valueOf(value), rs.getString(2));
  }

  @Test
  public void testCloseStatementWithoutQuery() throws SQLException {
    final Statement statement = sharedConnection.createStatement();
    // Make sure it is a streaming statement:
    statement.setFetchSize(Integer.MIN_VALUE);
    for (int count = 1; count <= 10; count++) {
      statement.close();
    }
  }

  @Test
  public void testClosePrepareStatementWithoutQuery() throws SQLException {
    final PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT 1");
    // Make sure it is a streaming statement:
    preparedStatement.setFetchSize(Integer.MIN_VALUE);
    for (int count = 1; count <= 10; count++) {
      preparedStatement.close();
    }
  }

  @Test
  public void testCloseStatement() throws SQLException {
    createTable("testStatementClose", "id int");
    final Statement statement = sharedConnection.createStatement();
    // Make sure it is a streaming statement:
    statement.setFetchSize(1);

    statement.execute("INSERT INTO testStatementClose (id) VALUES (1)");
    for (int count = 1; count <= 10; count++) {
      statement.close();
    }
  }

  @Test
  public void testClosePrepareStatement() throws SQLException {
    createTable("testPrepareStatementClose", "id int");
    sharedConnection
        .createStatement()
        .execute("INSERT INTO testPrepareStatementClose(id) VALUES (1),(2),(3)");
    final PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT * FROM testPrepareStatementClose");
    preparedStatement.execute();
    // Make sure it is a streaming statement:
    preparedStatement.setFetchSize(1);

    for (int count = 1; count <= 10; count++) {
      preparedStatement.close();
    }
  }

  @Test
  public void rewriteErrorRewriteValues() throws SQLException {
    prepareBatchUpdateException(true, true);
  }

  @Test
  public void rewriteErrorRewriteMulti() throws SQLException {
    prepareBatchUpdateException(false, true);
  }

  @Test
  public void rewriteErrorStandard() throws SQLException {
    prepareBatchUpdateException(false, false);
  }

  private void prepareBatchUpdateException(
      Boolean rewriteBatchedStatements, Boolean allowMultiQueries) throws SQLException {

    createTable("batchUpdateException", "i int,PRIMARY KEY (i)");
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", rewriteBatchedStatements.toString());
    props.setProperty("allowMultiQueries", allowMultiQueries.toString());
    props.setProperty("useServerPrepStmts", "false");

    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      verifyInsertCount(tmpConnection, 0);

      PreparedStatement ps =
          tmpConnection.prepareStatement("insert into batchUpdateException values(?)");
      ps.setInt(1, 1);
      ps.addBatch();
      ps.setInt(1, 2);
      ps.addBatch();
      ps.setInt(1, 1); // will fail, duplicate primary key
      ps.addBatch();
      ps.setInt(1, 3);
      ps.addBatch();

      try {
        ps.executeBatch();
        fail("exception should be throw above");
      } catch (BatchUpdateException bue) {
        int[] updateCounts = bue.getUpdateCounts();
        if (rewriteBatchedStatements
            || (sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2))) {
          assertEquals(4, updateCounts.length);
          assertEquals(Statement.EXECUTE_FAILED, updateCounts[0]);
          assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
          assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
          assertEquals(Statement.EXECUTE_FAILED, updateCounts[3]);
          verifyInsertCount(tmpConnection, 1);
        } else {
          assertEquals(4, updateCounts.length);
          assertEquals(1, updateCounts[0]);
          assertEquals(1, updateCounts[1]);
          assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
          assertEquals(1, updateCounts[3]);
          verifyInsertCount(tmpConnection, 4);
        }
        assertTrue(bue.getCause() instanceof SQLIntegrityConstraintViolationException);
      }
    }
  }

  /**
   * Test that using -1 (last prepared Statement), if next execution has parameter corresponding,
   * previous prepare will not be used.
   *
   * @throws Throwable if any error.
   */
  @Test
  public void testLastPrepareDiscarded() throws Throwable {

    PreparedStatement preparedStatement1 =
        sharedConnection.prepareStatement("INSERT INTO MultiTestA (data) VALUES (?)");
    preparedStatement1.setString(1, "A");
    preparedStatement1.execute();

    PreparedStatement preparedStatement2 =
        sharedConnection.prepareStatement("select * from (select ? `field1` from dual) as tt");
    preparedStatement2.setString(1, "B");
    try {
      preparedStatement2.execute();
      // must have thrown error if server prepare.
    } catch (Exception e) {
      // server prepare.
      ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM MultiTestA");
      assertTrue(rs.next());
      assertEquals("A", rs.getString(1));
      assertFalse(rs.next()); // "B" must not have been saved in Table MultiTestA
    }
  }

  @Test
  public void testMultiGeneratedKeyRewrite() throws Throwable {
    Assume.assumeFalse(isGalera());
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "true");
    props.setProperty("allowMultiQueries", "true");
    props.setProperty("useServerPrepStmts", "false");
    props.setProperty("sessionVariables", "auto_increment_increment=3");

    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      checkResults(tmpConnection);
      checkResultsPrepare(tmpConnection);
      checkResultsPrepareMulti(tmpConnection);
      checkResultsPrepareBatch(tmpConnection);
    }
  }

  @Test
  public void testMultiGeneratedKey() throws Throwable {
    Assume.assumeFalse(isGalera());
    Properties props = new Properties();
    props.setProperty("rewriteBatchedStatements", "false");
    props.setProperty("allowMultiQueries", "true");
    props.setProperty("useServerPrepStmts", "true");
    props.setProperty("sessionVariables", "auto_increment_increment=3");

    try (Connection tmpConnection = openNewConnection(connUri, props)) {
      checkResults(tmpConnection);
      checkResultsPrepare(tmpConnection);
      checkResultsPrepareMulti(tmpConnection);
      checkResultsPrepareBatch(tmpConnection);
    }
  }

  private void checkResultsPrepareBatch(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("truncate table testMultiGeneratedKey");

      // test single execution
      PreparedStatement preparedStatement =
          connection.prepareStatement(
              "INSERT INTO testMultiGeneratedKey (text) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS);
      preparedStatement.setString(1, "data1");
      preparedStatement.addBatch();
      preparedStatement.setString(1, "data2");
      preparedStatement.addBatch();

      int[] updates = preparedStatement.executeBatch();
      assertEquals(2, updates.length);

      ResultSet rs = preparedStatement.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));
      assertFalse(rs.next());
      assertFalse(preparedStatement.getMoreResults());

      assertEquals(1, updates[0]);
      assertEquals(1, updates[1]);
    }
  }

  private void checkResultsPrepare(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("truncate table testMultiGeneratedKey");

      // test single execution
      PreparedStatement preparedStatement =
          connection.prepareStatement(
              "INSERT INTO testMultiGeneratedKey (text) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS);
      preparedStatement.setString(1, "data1");
      int update = preparedStatement.executeUpdate();
      assertEquals(1, update);

      ResultSet rs = preparedStatement.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertFalse(rs.next());
    }
  }

  private void checkResultsPrepareMulti(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("truncate table testMultiGeneratedKey");

      // test single execution
      PreparedStatement preparedStatement =
          connection.prepareStatement(
              "INSERT INTO testMultiGeneratedKey (text) VALUES (?);"
                  + "INSERT INTO testMultiGeneratedKey (text) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS);
      preparedStatement.setString(1, "data1");
      preparedStatement.setString(2, "data2");
      int update = preparedStatement.executeUpdate();
      assertEquals(1, update);

      ResultSet rs = preparedStatement.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertFalse(rs.next());
      assertFalse(preparedStatement.getMoreResults());

      assertEquals(1, preparedStatement.getUpdateCount());
      rs = preparedStatement.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));
      assertFalse(rs.next());
      assertFalse(preparedStatement.getMoreResults());
    }
  }

  private void checkResults(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("truncate table testMultiGeneratedKey");

      // test single execution
      int update =
          stmt.executeUpdate(
              "INSERT INTO testMultiGeneratedKey (text) VALUES ('data1'), ('data2'), ('data3');"
                  + "INSERT INTO testMultiGeneratedKey (text) VALUES ('data4'), ('data5')",
              Statement.RETURN_GENERATED_KEYS);
      assertEquals(3, update);

      ResultSet rs = stmt.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertFalse(rs.next());

      assertFalse(stmt.getMoreResults());
      assertEquals(2, stmt.getUpdateCount());

      rs = stmt.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(13, rs.getInt(1));
      assertFalse(rs.next());

      assertFalse(stmt.getMoreResults());
      assertEquals(-1, stmt.getUpdateCount());

      update =
          stmt.executeUpdate(
              "INSERT INTO testMultiGeneratedKey (text) VALUES ('data11')",
              Statement.RETURN_GENERATED_KEYS);
      assertEquals(1, update);

      rs = stmt.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(16, rs.getInt(1));
      assertFalse(rs.next());
      assertFalse(stmt.getMoreResults());

      update =
          stmt.executeUpdate(
              "SELECT * FROM testMultiGeneratedKey", Statement.RETURN_GENERATED_KEYS);
      assertEquals(0, update);

      rs = stmt.getGeneratedKeys();
      assertFalse(rs.next());
      assertFalse(stmt.getMoreResults());

      // test batch
      stmt.executeQuery("truncate table testMultiGeneratedKey");
      stmt.addBatch(
          "INSERT INTO testMultiGeneratedKey (text) VALUES ('data0');INSERT INTO testMultiGeneratedKey (text) VALUES ('data1')");
      stmt.addBatch("INSERT INTO testMultiGeneratedKey (text) VALUES ('data2')");
      stmt.addBatch("INSERT INTO testMultiGeneratedKey (text) VALUES ('data3')");
      int[] updates = stmt.executeBatch();

      assertEquals(4, updates.length);

      assertEquals(1, updates[0]);
      assertEquals(1, updates[1]);
      assertEquals(1, updates[2]);
      assertEquals(1, updates[3]);

      rs = stmt.getGeneratedKeys();

      for (int i = 0; i < 4; i++) {
        assertTrue(rs.next());
        assertEquals(1 + i * 3, rs.getInt(1));
      }

      assertFalse(rs.next());
      assertFalse(stmt.getMoreResults());

      stmt.addBatch("INSERT INTO testMultiGeneratedKey (text) VALUES ('data11')");
      stmt.executeBatch();

      rs = stmt.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(13, rs.getInt(1));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testInsertSelectBulk() throws SQLException {

    try (Statement statement = sharedConnection.createStatement()) {
      statement.execute("DROP TABLE IF EXISTS testInsertSelectBulk");
      statement.execute("DROP TABLE IF EXISTS testInsertSelectBulk2");
      statement.execute("CREATE TABLE testInsertSelectBulk(col int, val int)");
      statement.execute("CREATE TABLE testInsertSelectBulk2(col int, val int)");
      statement.execute("INSERT INTO testInsertSelectBulk(col, val) VALUES (0,1), (2,3)");

      try (PreparedStatement preparedStatement =
          sharedConnection.prepareStatement(
              "INSERT INTO testInsertSelectBulk2(col, val) VALUES "
                  + "(?, (SELECT val FROM testInsertSelectBulk where col = ?))")) {

        preparedStatement.setInt(1, 4);
        preparedStatement.setInt(2, 0);
        preparedStatement.addBatch();

        preparedStatement.setInt(1, 5);
        preparedStatement.setInt(2, 2);
        preparedStatement.addBatch();

        preparedStatement.executeBatch();
      }

      ResultSet rs = statement.executeQuery("SELECT * from testInsertSelectBulk2");
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));
      assertEquals(1, rs.getInt(2));

      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(3, rs.getInt(2));
    }
  }

  @Test
  public void testAffectedRow() throws SQLException {
    createTable("testAffectedRow", "id int");
    testAffectedRow(false);
    testAffectedRow(true);
  }

  private void testAffectedRow(boolean useAffectedRows) throws SQLException {
    try (Connection con = setConnection(useAffectedRows ? "&useAffectedRows" : "")) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE testAffectedRow");
      stmt.execute("INSERT INTO testAffectedRow values (1), (1), (2), (3)");
      int rowCount = stmt.executeUpdate("UPDATE testAffectedRow set id = 1");
      assertEquals(useAffectedRows ? 2 : 4, rowCount);
    }
  }

  @Test
  public void shouldDuplicateKeyUpdateNotReturnExtraRows() throws Throwable {
    try (Connection con = openNewConnection(connUri, new Properties())) {
      try (Statement stmt = con.createStatement()) {
        stmt.execute("truncate table testMultiGeneratedKey");
      }
      try (PreparedStatement pstmt =
          con.prepareStatement(
              "INSERT INTO testMultiGeneratedKey (id, text) VALUES (?, ?) "
                  + "ON DUPLICATE KEY UPDATE text = VALUES(text)",
              Statement.RETURN_GENERATED_KEYS)) {
        // Insert a row.
        pstmt.setInt(1, 1);
        pstmt.setString(2, "initial");
        assertEquals(1, pstmt.executeUpdate());
        try (ResultSet rs = pstmt.getGeneratedKeys()) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertFalse(rs.next());
        }
        // Update the row.
        pstmt.setInt(1, 1);
        pstmt.setString(2, "updated");
        assertEquals(2, pstmt.executeUpdate());
        try (ResultSet rs = pstmt.getGeneratedKeys()) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void insertBatchMultiInsert() throws Throwable {
    Properties properties = new Properties();
    properties.setProperty("allowMultiQueries", "true");
    try (Connection con = openNewConnection(connUri, properties)) {
      try (Statement stmt = con.createStatement()) {
        stmt.execute("truncate table testMultiGeneratedKey");
      }
      try (PreparedStatement pstmt =
          con.prepareStatement(
              "INSERT INTO testMultiGeneratedKey (text) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS)) {
        // Insert a row.
        pstmt.setString(1, "initial");
        pstmt.addBatch();
        pstmt.setString(1, "updated");
        pstmt.addBatch();
        pstmt.executeBatch();
        try (ResultSet rs = pstmt.getGeneratedKeys()) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void multiInsertReturnOneGenerated() throws Throwable {
    try (Connection con = openNewConnection(connUri, new Properties())) {
      try (Statement stmt = con.createStatement()) {
        stmt.execute("truncate table testMultiGeneratedKey");
      }
      try (PreparedStatement pstmt =
          con.prepareStatement(
              "INSERT INTO testMultiGeneratedKey (text) VALUES (?), (?)",
              Statement.RETURN_GENERATED_KEYS)) {
        // Insert a row.
        pstmt.setString(1, "initial");
        pstmt.setString(2, "updated");
        pstmt.executeUpdate();
        try (ResultSet rs = pstmt.getGeneratedKeys()) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testAffectedRowBatch() throws SQLException {
    createTable("testAffectedRowBatch", "id int PRIMARY KEY, data varchar(10)");
    testAffectedRowBatch(false);
    testAffectedRowBatch(true);

    testAffectedRowLongBatch(false);
    testAffectedRowLongBatch(true);
  }

  private void testAffectedRowBatch(boolean useAffectedRows) throws SQLException {
    try (Connection con =
        setConnection("&rewriteBatchedStatements" + (useAffectedRows ? "&useAffectedRows" : ""))) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE testAffectedRowBatch");
      try (PreparedStatement preparedStatement =
          con.prepareStatement("INSERT INTO testAffectedRowBatch values (?, ?)")) {
        preparedStatement.setInt(1, 1);
        preparedStatement.setString(2, "1");
        preparedStatement.addBatch();
        preparedStatement.setInt(1, 2);
        preparedStatement.setString(2, "0");
        preparedStatement.addBatch();
        int[] res = preparedStatement.executeBatch();
        assertEquals(2, res.length);
        assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, res[1]);

        preparedStatement.setInt(1, 3);
        preparedStatement.setString(2, "0");
        preparedStatement.addBatch();
        res = preparedStatement.executeBatch();
        assertEquals(1, res.length);
        assertEquals(1, res[0]);
      }
      try (PreparedStatement preparedStatement =
          con.prepareStatement(
              "INSERT INTO testAffectedRowBatch (id) values (?) ON DUPLICATE KEY UPDATE data = '0'")) {
        preparedStatement.setInt(1, 1);
        preparedStatement.addBatch();
        preparedStatement.setInt(1, 2);
        preparedStatement.addBatch();
        int[] res = preparedStatement.executeBatch();

        assertEquals(2, res.length);
        assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, res[1]);

        preparedStatement.setInt(1, 6);
        preparedStatement.addBatch();
        res = preparedStatement.executeBatch();

        assertEquals(1, res.length);
        assertEquals(1, res[0]);

        preparedStatement.setInt(1, 1);

        preparedStatement.addBatch();
        preparedStatement.setInt(1, 2);
        preparedStatement.addBatch();
        res = preparedStatement.executeBatch();
        assertEquals(2, res.length);
        if (useAffectedRows) {
          assertEquals(0, res[0]);
          assertEquals(0, res[1]);
        } else {
          assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
          assertEquals(Statement.SUCCESS_NO_INFO, res[1]);
        }
      }
    }
  }

  private void testAffectedRowLongBatch(boolean useAffectedRows) throws SQLException {
    try (Connection con =
        setConnection("&rewriteBatchedStatements" + (useAffectedRows ? "&useAffectedRows" : ""))) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE testAffectedRowBatch");
      try (PreparedStatement preparedStatement =
          con.prepareStatement("INSERT INTO testAffectedRowBatch values (?, ?)")) {
        preparedStatement.setInt(1, 1);
        preparedStatement.setString(2, "1");
        preparedStatement.addBatch();
        preparedStatement.setInt(1, 2);
        preparedStatement.setString(2, "0");
        preparedStatement.addBatch();
        long[] res = preparedStatement.executeLargeBatch();
        assertEquals(2, res.length);
        assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, res[1]);

        preparedStatement.setInt(1, 3);
        preparedStatement.setString(2, "0");
        preparedStatement.addBatch();
        res = preparedStatement.executeLargeBatch();
        assertEquals(1, res.length);
        assertEquals(1, res[0]);
      }
      try (PreparedStatement preparedStatement =
          con.prepareStatement(
              "INSERT INTO testAffectedRowBatch (id) values (?) ON DUPLICATE KEY UPDATE data = '0'")) {
        preparedStatement.setInt(1, 1);
        preparedStatement.addBatch();
        preparedStatement.setInt(1, 2);
        preparedStatement.addBatch();
        long[] res = preparedStatement.executeLargeBatch();

        assertEquals(2, res.length);
        assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, res[1]);

        preparedStatement.setInt(1, 6);
        preparedStatement.addBatch();
        res = preparedStatement.executeLargeBatch();

        assertEquals(1, res.length);
        assertEquals(1, res[0]);

        preparedStatement.setInt(1, 1);

        preparedStatement.addBatch();
        preparedStatement.setInt(1, 2);
        preparedStatement.addBatch();
        res = preparedStatement.executeLargeBatch();
        assertEquals(2, res.length);
        if (useAffectedRows) {
          assertEquals(0, res[0]);
          assertEquals(0, res[1]);
        } else {
          assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
          assertEquals(Statement.SUCCESS_NO_INFO, res[1]);
        }
      }
    }
  }
}
