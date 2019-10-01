/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicBatchTest extends BaseTest {

  /**
   * Tables initialisation.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("test_batch", "id int not null primary key auto_increment, test varchar(10)");
    createTable("test_batch2", "id int not null primary key auto_increment, test varchar(10)");
    createTable("test_batch3", "id int not null primary key auto_increment, test varchar(10)");
    createTable("batchUpdateException", "i int,PRIMARY KEY (i)");
    createTable("batchPrepareUpdateException", "i int,PRIMARY KEY (i)");
    createTable(
        "rewritetest", "id int not null primary key, a varchar(10), b int", "engine=innodb");
    createTable(
        "rewritetest2", "id int not null primary key, a varchar(10), b int", "engine=innodb");
    createTable("bug501452", "id int not null primary key, value varchar(20)");
  }

  @Test
  public void batchTest() throws SQLException {
    Assume.assumeFalse(sharedIsRewrite());
    PreparedStatement ps =
        sharedConnection.prepareStatement(
            "insert into test_batch values (null, ?)", Statement.RETURN_GENERATED_KEYS);
    ps.setString(1, "aaa");
    ps.addBatch();
    ps.setString(1, "bbb");
    ps.addBatch();
    ps.setString(1, "ccc");
    ps.addBatch();
    int[] batchResult = ps.executeBatch();
    ResultSet rs1 = ps.getGeneratedKeys();

    int[] autoInc = setAutoInc();

    for (int count = 1; count <= 3; count++) {
      assertTrue(rs1.next());
      assertTrue(
          String.valueOf(autoInc[0] * count + autoInc[1]).equalsIgnoreCase(rs1.getString(1)));
    }
    for (int unitInsertNumber : batchResult) {
      assertEquals(1, unitInsertNumber);
    }
    ps.setString(1, "aaa");
    ps.addBatch();
    ps.setString(1, "bbb");
    ps.addBatch();
    ps.setString(1, "ccc");
    ps.addBatch();
    batchResult = ps.executeBatch();
    for (int unitInsertNumber : batchResult) {
      assertEquals(1, unitInsertNumber);
    }
    final ResultSet rs =
        sharedConnection.createStatement().executeQuery("select * from test_batch");
    ps.executeQuery("SELECT 1");
    try {
      rs1 = ps.getGeneratedKeys();
      fail();
    } catch (SQLException sqle) {
      assertEquals(
          "Cannot return generated keys : query was not set with Statement.RETURN_GENERATED_KEYS",
          sqle.getMessage());
    }
    assertFalse(rs1.next());
    assertEquals(true, rs.next());
    assertEquals("aaa", rs.getString(2));
    assertEquals(true, rs.next());
    assertEquals("bbb", rs.getString(2));
    assertEquals(true, rs.next());
    assertEquals("ccc", rs.getString(2));
  }

  @Test
  public void batchTestStmtUsingPipeline() throws SQLException {
    batchTestStmt(sharedConnection);
  }

  @Test
  public void batchTestStmtWithoutPipeline() throws SQLException {
    try (Connection connection = setConnection("&useBatchMultiSend=false")) {
      batchTestStmt(connection);
    }
  }

  private void batchTestStmt(Connection connection) throws SQLException {
    Statement stmt = connection.createStatement();
    stmt.execute("truncate test_batch2");
    stmt.addBatch("insert into test_batch2 values (null, 'hej1')");
    stmt.addBatch("insert into test_batch2 values (null, 'hej2')");
    stmt.addBatch("insert into test_batch2 values (null, 'hej3')");
    stmt.addBatch("insert into test_batch2 values (null, 'hej4')");
    int[] inserts = stmt.executeBatch();

    assertEquals(4, inserts.length);
    assertEquals(1, inserts[0]);
    assertEquals(1, inserts[1]);
    assertEquals(1, inserts[2]);
    assertEquals(1, inserts[3]);

    int[] autoInc = setAutoInc();

    ResultSet rs = stmt.executeQuery("select * from test_batch2");
    for (int i = 1; i <= 4; i++) {
      assertEquals(true, rs.next());
      assertEquals(autoInc[1] + i * autoInc[0], rs.getInt(1));
      assertEquals("hej" + i, rs.getString(2));
    }
    assertEquals(false, rs.next());
  }

  @Test
  public void batchUpdateException() throws Exception {
    Statement st = sharedConnection.createStatement();
    st.addBatch("insert into batchUpdateException values(1)");
    st.addBatch("insert into batchUpdateException values(2)");
    st.addBatch("insert into batchUpdateException values(1)"); // will fail, duplicate primary key
    st.addBatch("insert into batchUpdateException values(3)");

    try {
      st.executeBatch();
      fail("exception should be throw above");
    } catch (BatchUpdateException bue) {
      int[] updateCounts = bue.getUpdateCounts();
      assertEquals(4, updateCounts.length);
      if (sharedIsRewrite()) {
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[3]);
      } else {
        // prepare or allowMultiQueries options
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
        assertEquals(1, updateCounts[3]);
      }
      assertTrue(bue.getCause() instanceof SQLIntegrityConstraintViolationException);
    }
  }

  @Test
  public void batchPrepareUpdateException() throws Exception {
    PreparedStatement st =
        sharedConnection.prepareStatement("insert into batchPrepareUpdateException values(?)");
    st.setInt(1, 1);
    st.addBatch();
    st.setInt(1, 2);
    st.addBatch();
    st.setInt(1, 1); // will fail, duplicate primary key
    st.addBatch();
    st.setInt(1, 3);
    st.addBatch();

    try {
      st.executeBatch();
      fail("exception should be throw above");
    } catch (BatchUpdateException bue) {
      int[] updateCounts = bue.getUpdateCounts();
      assertEquals(4, updateCounts.length);
      if (sharedIsRewrite()
          || (sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2))) {
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[0]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[3]);
      } else {
        // prepare or allowMultiQueries options
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
        assertEquals(1, updateCounts[3]);
      }
      assertTrue(bue.getCause() instanceof SQLIntegrityConstraintViolationException);
    }
  }

  @Test
  public void testBatchLoop() throws SQLException {
    PreparedStatement ps =
        sharedConnection.prepareStatement("insert into rewritetest values (?,?,?)");
    for (int i = 0; i < 10; i++) {
      ps.setInt(1, i);
      ps.setString(2, "bbb" + i);
      ps.setInt(3, 30 + i);
      ps.addBatch();
    }
    ps.executeBatch();
    ResultSet rs = sharedConnection.createStatement().executeQuery("select * from rewritetest");
    int counter = 0;
    while (rs.next()) {
      assertEquals(counter++, rs.getInt("id"));
    }
    assertEquals(10, counter);
  }

  @Test
  public void testBatchLoopWithDupKey() throws SQLException {
    PreparedStatement ps =
        sharedConnection.prepareStatement(
            "insert into rewritetest2 values (?,?,?) on duplicate key update a=values(a)");
    for (int i = 0; i < 2; i++) {
      ps.setInt(1, 0);
      ps.setString(2, "bbb" + i);
      ps.setInt(3, 30 + i);
      ps.addBatch();
    }
    ps.executeBatch();

    ResultSet rs = sharedConnection.createStatement().executeQuery("select * from rewritetest2");
    int counter = 0;
    while (rs.next()) {
      assertEquals(counter++, rs.getInt("id"));
    }
    assertEquals(1, counter);
  }

  @Test
  public void testBug501452() throws SQLException {
    PreparedStatement ps =
        sharedConnection.prepareStatement("insert into bug501452 (id,value) values (?,?)");
    ps.setObject(1, 1);
    ps.setObject(2, "value for 1");
    ps.addBatch();

    ps.executeBatch();

    ps.setObject(1, 2);
    ps.setObject(2, "value for 2");
    ps.addBatch();

    ps.executeBatch();
  }

  @Test
  public void testMultipleStatementBatch() throws SQLException {
    try (Connection connection =
        setConnection("&sessionVariables=auto_increment_increment=2&allowMultiQueries=true")) {
      Statement stmt = connection.createStatement();
      stmt.addBatch("INSERT INTO test_batch3(test) value ('a')");
      stmt.addBatch("INSERT INTO test_batch3(test) value ('b')");
      stmt.addBatch("INSERT INTO test_batch3(test) value ('a'), ('e')");
      stmt.addBatch("UPDATE test_batch3 set test='c' WHERE test = 'a'");
      stmt.addBatch("UPDATE test_batch3 set test='d' WHERE test = 'b'");
      stmt.addBatch("INSERT INTO test_batch3(test) value ('e')");

      int[] updateCount = stmt.executeBatch();
      assertEquals(6, updateCount.length);
      assertEquals(1, updateCount[0]);
      assertEquals(1, updateCount[1]);
      assertEquals(2, updateCount[2]);
      assertEquals(2, updateCount[3]);
      assertEquals(1, updateCount[4]);
      assertEquals(1, updateCount[5]);

      assertEquals(-1, stmt.getUpdateCount());
      assertFalse(stmt.getMoreResults());

      ResultSet resultSet = stmt.getGeneratedKeys();

      int[] autoInc = setAutoInc(2, -1);

      assertTrue(resultSet.next());
      assertEquals(autoInc[1] + autoInc[0], resultSet.getInt(1));
      assertTrue(resultSet.next());
      assertEquals(autoInc[1] + 2 * autoInc[0], resultSet.getInt(1));
      assertTrue(resultSet.next());
      assertEquals(autoInc[1] + 3 * autoInc[0], resultSet.getInt(1));
      assertTrue(resultSet.next());
      assertEquals(autoInc[1] + 4 * autoInc[0], resultSet.getInt(1));
      assertTrue(resultSet.next());
      assertEquals(autoInc[1] + 5 * autoInc[0], resultSet.getInt(1));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testBatchString() throws SQLException {
    Assume.assumeTrue(
        runLongTest
            && (sharedOptions().useBulkStmts || sharedIsRewrite())); // if not will be too long.
    createTable("testBatchString", "charValue VARCHAR(100) NOT NULL");
    Statement stmt = sharedConnection.createStatement();
    String[] datas = new String[1_000_000];
    String empty =
        "____________________________________________________________________________________________________";

    for (int i = 0; i < datas.length; i++) {
      datas[i] = (i + empty).substring(0, 100);
    }

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("INSERT INTO testBatchString (charValue) values (?)")) {
      for (String data : datas) {
        preparedStatement.setString(1, data); // a random 100 byte data
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }

    stmt.setFetchSize(100);
    ResultSet rs = stmt.executeQuery("SELECT charValue FROM testBatchString");
    int counter = 0;
    while (rs.next()) {
      assertEquals(datas[counter++], rs.getString(1));
    }
    assertEquals(datas.length, counter);
  }
}
