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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResultSetTest extends BaseTest {

  /**
   * Initialisation.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("result_set_test", "id int not null primary key auto_increment, name char(20)");
  }

  @Test
  public void isBeforeFirstFetchTest() throws SQLException {
    insertRows(1);
    Statement statement = sharedConnection.createStatement();
    statement.setFetchSize(1);
    ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
    assertTrue(resultSet.isBeforeFirst());
    while (resultSet.next()) {
      assertFalse(resultSet.isBeforeFirst());
    }
    assertFalse(resultSet.isBeforeFirst());
    resultSet.close();
    try {
      resultSet.isBeforeFirst();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  /**
   * CONJ-424: Calling getGeneratedKeys() two times on the same connection, with different
   * PreparedStatement on a table that does not have an auto increment.
   */
  @Test
  public void testGeneratedKeysWithoutTableAutoIncrementCalledTwice() throws SQLException {
    createTable("gen_key_test_resultset", "name VARCHAR(40) NOT NULL, xml MEDIUMTEXT");
    String sql = "INSERT INTO gen_key_test_resultset (name, xml) VALUES (?, ?)";

    for (int i = 0; i < 2; i++) {
      try (PreparedStatement preparedStatement =
          sharedConnection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

        preparedStatement.setString(1, "John");
        preparedStatement.setString(2, "<xml/>");
        preparedStatement.executeUpdate();

        try (ResultSet generatedKeysResultSet = preparedStatement.getGeneratedKeys()) {
          assertFalse(generatedKeysResultSet.next());
        }
      }
    }
  }

  @Test
  public void isBeforeFirstFetchZeroRowsTest() throws SQLException {
    insertRows(2);
    Statement statement = sharedConnection.createStatement();
    statement.setFetchSize(1);
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test")) {
      assertTrue(resultSet.isBeforeFirst());
      assertTrue(resultSet.next());
      assertFalse(resultSet.isBeforeFirst());
      resultSet.close();
      try {
        resultSet.isBeforeFirst();
        fail("The above row should have thrown an SQLException");
      } catch (SQLException e) {
        // Make sure an exception has been thrown informing us that the ResultSet was closed
        assertTrue(e.getMessage().contains("closed"));
      }
    }
  }

  @Test
  public void isClosedTest() throws SQLException {
    insertRows(1);
    ResultSet resultSet =
        sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
    assertFalse(resultSet.isClosed());
    while (resultSet.next()) {
      assertFalse(resultSet.isClosed());
    }
    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  public void isBeforeFirstTest() throws SQLException {
    insertRows(1);
    ResultSet resultSet =
        sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
    assertTrue(resultSet.isBeforeFirst());
    while (resultSet.next()) {
      assertFalse(resultSet.isBeforeFirst());
    }
    assertFalse(resultSet.isBeforeFirst());
    resultSet.close();
    try {
      resultSet.isBeforeFirst();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  @Test
  public void isFirstZeroRowsTest() throws SQLException {
    insertRows(0);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet resultSet = stmt.executeQuery("SELECT * FROM result_set_test");
    assertFalse(resultSet.isFirst());
    assertFalse(resultSet.next()); // No more rows after this
    assertFalse(resultSet.isFirst()); // connectorj compatibility
    assertFalse(resultSet.first());
    resultSet.close();
    try {
      resultSet.isFirst();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  @Test
  public void isFirstOneRowsTest() throws SQLException {
    insertRows(1);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet resultSet = stmt.executeQuery("SELECT * FROM result_set_test");
    assertFalse(resultSet.isFirst());
    assertTrue(resultSet.next());
    assertTrue(resultSet.isFirst());
    assertFalse(resultSet.next());
    assertFalse(resultSet.isFirst());
    assertTrue(resultSet.first());
    assertEquals(1, resultSet.getInt(1));
    resultSet.close();
    try {
      resultSet.isFirst();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  @Test
  public void isFirstTwoRowsTest() throws SQLException {
    isFirstTwoRowsTest(true);
    isFirstTwoRowsTest(false);
  }

  private void isFirstTwoRowsTest(boolean fetching) throws SQLException {
    insertRows(2);
    try (Statement statement =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      if (fetching) {
        statement.setFetchSize(1);
      }
      ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
      assertFalse(resultSet.isFirst());
      assertTrue(resultSet.next());
      assertTrue(resultSet.isFirst());
      assertTrue(resultSet.next());
      assertFalse(resultSet.isFirst());
      assertFalse(resultSet.next()); // No more rows after this
      assertFalse(resultSet.isFirst());
      assertTrue(resultSet.first());
      assertEquals(1, resultSet.getInt(1));
      resultSet.close();
      try {
        resultSet.isFirst();
        fail("The above row should have thrown an SQLException");
      } catch (SQLException e) {
        // Make sure an exception has been thrown informing us that the ResultSet was closed
        assertTrue(e.getMessage().contains("closed"));
      }
    }
  }

  @Test
  public void isLastZeroRowsTest() throws SQLException {
    insertRows(0);
    ResultSet resultSet =
        sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
    assertFalse(resultSet.isLast()); // connectorj compatibility
    assertFalse(resultSet.next()); // No more rows after this
    assertFalse(resultSet.isLast());
    assertFalse(resultSet.last());
    resultSet.close();
    try {
      resultSet.isLast();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  @Test
  public void isLastOneRowsTest() throws SQLException {
    insertRows(1);
    ResultSet resultSet =
        sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
    assertFalse(resultSet.isLast());
    assertTrue(resultSet.next());
    assertTrue(resultSet.isLast());
    assertTrue(resultSet.last());
    assertFalse(resultSet.next()); // No more rows after this
    assertFalse(resultSet.isLast());
    assertTrue(resultSet.last());

    resultSet.close();
    try {
      resultSet.isLast();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  @Test
  public void isLastTwoRowsTest() throws SQLException {
    isLastTwoRowsTest(true);
    isLastTwoRowsTest(false);
  }

  private void isLastTwoRowsTest(boolean fetching) throws SQLException {
    insertRows(2);
    try (Statement statement = sharedConnection.createStatement()) {
      if (fetching) {
        statement.setFetchSize(1);
      }
      ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
      assertFalse(resultSet.isLast());
      assertTrue(resultSet.next());
      assertFalse(resultSet.isLast());
      assertTrue(resultSet.next());
      assertTrue(resultSet.isLast());
      assertFalse(resultSet.next()); // No more rows after this
      assertFalse(resultSet.isLast());
      assertTrue(resultSet.last());
      assertEquals(2, resultSet.getInt(1));
      resultSet.close();
      try {
        resultSet.isLast();
        fail("The above row should have thrown an SQLException");
      } catch (SQLException e) {
        // Make sure an exception has been thrown informing us that the ResultSet was closed
        assertTrue(e.getMessage().contains("closed"));
      }
    }
  }

  @Test
  public void isAfterLastZeroRowsTest() throws SQLException {
    insertRows(0);
    ResultSet resultSet =
        sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
    assertFalse(resultSet.isAfterLast());
    assertFalse(resultSet.next()); // No more rows after this
    assertFalse(resultSet.isAfterLast());
    resultSet.close();
    try {
      resultSet.isAfterLast();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  @Test
  public void isAfterLastTwoRowsTest() throws SQLException {
    insertRows(2);
    ResultSet resultSet =
        sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
    assertFalse(resultSet.isAfterLast());
    assertTrue(resultSet.next());
    assertFalse(resultSet.isAfterLast());
    assertTrue(resultSet.next());
    assertFalse(resultSet.isAfterLast());
    assertFalse(resultSet.next()); // No more rows after this
    assertTrue(resultSet.isAfterLast());
    assertTrue(resultSet.last());
    assertEquals(2, resultSet.getInt(1));
    resultSet.close();
    try {
      resultSet.isAfterLast();
      fail("The above row should have thrown an SQLException");
    } catch (SQLException e) {
      // Make sure an exception has been thrown informing us that the ResultSet was closed
      assertTrue(e.getMessage().contains("closed"));
    }
  }

  @Test
  public void previousTest() throws SQLException {
    insertRows(2);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    try (ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test")) {
      assertFalse(rs.previous());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertFalse(rs.previous());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertTrue(rs.previous());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.last());
      assertEquals(2, rs.getInt(1));
    }
  }

  @Test
  public void firstTest() throws SQLException {
    insertRows(2);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.first());
    assertTrue(rs.isFirst());
    rs.close();
    try {
      rs.first();
      fail("cannot call first() on a closed result set");
    } catch (SQLException sqlex) {
      // eat exception
    }
  }

  @Test
  public void lastTest() throws SQLException {
    insertRows(2);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
    assertTrue(rs.last());
    assertTrue(rs.isLast());
    assertFalse(rs.next());
    rs.first();
    rs.close();
    try {
      rs.last();
      fail("cannot call last() on a closed result set");
    } catch (SQLException sqlex) {
      // eat exception
    }
  }

  @Test
  public void relativeForwardTest() throws SQLException {
    insertRows(3);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
    assertTrue(rs.isBeforeFirst());
    assertFalse(rs.isFirst());
    assertFalse(rs.isLast());
    assertFalse(rs.isAfterLast());

    assertTrue(rs.relative(2));
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isFirst());
    assertFalse(rs.isLast());
    assertFalse(rs.isAfterLast());

    assertTrue(rs.relative(1));
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isFirst());
    assertTrue(rs.isLast());
    assertFalse(rs.isAfterLast());

    assertFalse(rs.relative(1));
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isFirst());
    assertFalse(rs.isLast());
    assertTrue(rs.isAfterLast());

    assertFalse(rs.next());
    rs.close();
  }

  @Test
  public void relativeBackwardTest() throws SQLException {
    insertRows(3);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
    rs.afterLast();
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isFirst());
    assertFalse(rs.isLast());
    assertTrue(rs.isAfterLast());

    assertTrue(rs.relative(-2));
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isFirst());
    assertFalse(rs.isLast());
    assertFalse(rs.isAfterLast());

    assertTrue(rs.relative(-1));
    assertFalse(rs.isBeforeFirst());
    assertTrue(rs.isFirst());
    assertFalse(rs.isLast());
    assertFalse(rs.isAfterLast());

    assertFalse(rs.relative(-1));
    assertTrue(rs.isBeforeFirst());
    assertFalse(rs.isFirst());
    assertFalse(rs.isLast());
    assertFalse(rs.isAfterLast());

    assertFalse(rs.previous());
    rs.close();
  }

  private void insertRows(int numberOfRowsToInsert) throws SQLException {
    sharedConnection.createStatement().execute("truncate result_set_test ");
    for (int i = 1; i <= numberOfRowsToInsert; i++) {
      sharedConnection
          .createStatement()
          .executeUpdate("INSERT INTO result_set_test VALUES(" + i + ", 'row" + i + "')");
    }
  }

  /**
   * CONJ-403: NPE in getGenerated keys.
   *
   * @throws SQLException if error occur
   */
  @Test
  public void generatedKeyNpe() throws SQLException {
    createTable("generatedKeyNpe", "id int not null primary key auto_increment, val int");
    Statement statement = sharedConnection.createStatement();
    statement.execute(
        "INSERT INTO generatedKeyNpe(val) values (0)", Statement.RETURN_GENERATED_KEYS);
    try (ResultSet rs = statement.getGeneratedKeys()) {
      assertTrue(rs.next());
    }
  }

  @Test
  public void generatedKeyError() throws SQLException {
    createTable("generatedKeyNpe", "id int not null primary key auto_increment, val int");
    Statement statement = sharedConnection.createStatement();
    statement.execute("INSERT INTO generatedKeyNpe(val) values (0)");
    try {
      statement.getGeneratedKeys();
      fail();
    } catch (SQLException sqle) {
      assertEquals(
          "Cannot return generated keys : query was not set with Statement.RETURN_GENERATED_KEYS",
          sqle.getMessage());
    }
  }

  @Test
  public void testResultSetAbsolute() throws Exception {
    insertRows(50);
    try (Statement statement =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      statement.setFetchSize(10);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
        assertFalse(rs.absolute(52));
        assertFalse(rs.absolute(-52));

        assertTrue(rs.absolute(42));
        assertEquals("row42", rs.getString(2));

        assertTrue(rs.absolute(-11));
        assertEquals("row40", rs.getString(2));

        assertTrue(rs.absolute(0));
        assertTrue(rs.isBeforeFirst());

        assertFalse(rs.absolute(51));
        assertTrue(rs.isAfterLast());

        assertTrue(rs.absolute(-1));
        assertEquals("row50", rs.getString(2));

        assertTrue(rs.absolute(-50));
        assertEquals("row1", rs.getString(2));
      }
    }
  }

  @Test
  public void testResultSetIsAfterLast() throws Exception {
    insertRows(2);
    try (Statement statement = sharedConnection.createStatement()) {
      statement.setFetchSize(1);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertTrue(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.next());
        assertFalse(rs.isLast());
        assertTrue(rs.isAfterLast());
      }

      insertRows(0);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isLast());
        assertFalse(rs.next());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast()); // jdbc indicate that results with no rows return false.
      }
    }
  }

  @Test
  public void testResultSetAfterLast() throws Exception {
    try (Statement statement = sharedConnection.createStatement()) {
      checkLastResultSet(statement);
      statement.setFetchSize(1);
      checkLastResultSet(statement);
    }
  }

  private void checkLastResultSet(Statement statement) throws SQLException {

    insertRows(10);
    try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {

      assertTrue(rs.last());
      assertFalse(rs.isAfterLast());
      assertTrue(rs.isLast());

      rs.afterLast();
      assertTrue(rs.isAfterLast());
      assertFalse(rs.isLast());
    }

    insertRows(0);
    try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {

      assertFalse(rs.last());
      assertFalse(rs.isAfterLast());
      assertFalse(rs.isLast());

      rs.afterLast();
      assertFalse(rs.isAfterLast()); // jdbc indicate that results with no rows return false.
      assertFalse(rs.isLast());
    }
  }

  @Test
  public void testStreamInsensitive() throws Exception {
    createTable("testStreamInsensitive", "s1 varchar(20)");

    for (int r = 0; r < 20; r++) {
      sharedConnection
          .createStatement()
          .executeUpdate("insert into testStreamInsensitive values('V" + r + "')");
    }
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(10);

    // reading forward
    ResultSet rs = stmt.executeQuery("select * from testStreamInsensitive");
    for (int i = 0; i < 20; i++) {
      assertTrue(rs.next());
      assertEquals("V" + i, rs.getString(1));
    }
    assertFalse(rs.next());

    rs = stmt.executeQuery("select * from testStreamInsensitive");
    for (int i = 0; i < 20; i++) {
      assertFalse(rs.isAfterLast());
      assertTrue(rs.next());
      assertEquals("V" + i, rs.getString(1));
      assertFalse(rs.isAfterLast());
    }
    assertFalse(rs.isAfterLast());
    assertFalse(rs.next());
    assertTrue(rs.isAfterLast());

    rs = stmt.executeQuery("select * from testStreamInsensitive");
    assertTrue(rs.absolute(20));
    assertEquals("V19", rs.getString(1));
    assertFalse(rs.isAfterLast());
    assertFalse(rs.absolute(21));
    assertTrue(rs.isAfterLast());

    // reading backward
    rs = stmt.executeQuery("select * from testStreamInsensitive");
    rs.afterLast();
    for (int i = 19; i >= 0; i--) {
      assertTrue(rs.previous());
      assertEquals("V" + i, rs.getString(1));
    }
    assertFalse(rs.previous());

    rs = stmt.executeQuery("select * from testStreamInsensitive");
    assertTrue(rs.last());
    assertEquals("V19", rs.getString(1));

    assertTrue(rs.first());
    assertEquals("V0", rs.getString(1));
  }

  @Test
  public void testStreamForward() throws Exception {
    createTable("testStreamForward", "s1 varchar(20)");

    for (int r = 0; r < 20; r++) {
      sharedConnection
          .createStatement()
          .executeUpdate("insert into testStreamForward values('V" + r + "')");
    }
    Statement stmt =
        sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(10);

    // reading forward
    ResultSet rs = stmt.executeQuery("select * from testStreamForward");
    for (int i = 0; i < 20; i++) {
      assertTrue(rs.next());
      assertEquals("V" + i, rs.getString(1));
    }
    assertFalse(rs.next());

    // checking isAfterLast that may need to fetch next result
    rs = stmt.executeQuery("select * from testStreamForward");
    for (int i = 0; i < 20; i++) {
      assertFalse(rs.isAfterLast());
      assertTrue(rs.next());
      assertEquals("V" + i, rs.getString(1));
      assertFalse(rs.isAfterLast());
    }
    assertFalse(rs.isAfterLast());
    assertFalse(rs.next());
    assertTrue(rs.isAfterLast());

    // reading backward
    rs = stmt.executeQuery("select * from testStreamForward");
    rs.afterLast();
    try {
      rs.previous();
      fail("Must have thrown exception since previous is not possible when fetching");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("Invalid operation for result set type TYPE_FORWARD_ONLY"));
    }

    rs = stmt.executeQuery("select * from testStreamForward");
    assertTrue(rs.last());
    assertEquals("V19", rs.getString(1));

    try {
      rs.first();
      fail("Must have thrown exception since previous is not possible when fetching");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("Invalid operation for result set type TYPE_FORWARD_ONLY"));
    }
  }

  /**
   * [CONJ-437] getString on field with ZEROFILL doesn't have the '0' leading chars when using
   * binary protocol.
   *
   * @throws SQLException if any abnormal error occur
   */
  @Test
  public void leadingZeroTest() throws SQLException {
    createTable(
        "leadingZero",
        "t1 TINYINT(3) unsigned zerofill"
            + ", t2 TINYINT(8) unsigned zerofill"
            + ", t3 TINYINT unsigned zerofill"
            + ", t4 smallint(3) unsigned zerofill"
            + ", t5 smallint(8) unsigned zerofill"
            + ", t6 smallint unsigned zerofill"
            + ", t7 MEDIUMINT(3) unsigned zerofill"
            + ", t8 MEDIUMINT(8) unsigned zerofill"
            + ", t9 MEDIUMINT unsigned zerofill"
            + ", t10 INT(3) unsigned zerofill"
            + ", t11 INT(8) unsigned zerofill"
            + ", t12 INT unsigned zerofill"
            + ", t13 BIGINT(3) unsigned zerofill"
            + ", t14 BIGINT(8) unsigned zerofill"
            + ", t15 BIGINT unsigned zerofill"
            + ", t16 DECIMAL(6,3) unsigned zerofill"
            + ", t17 DECIMAL(11,3) unsigned zerofill"
            + ", t18 DECIMAL unsigned zerofill"
            + ", t19 FLOAT(6,3) unsigned zerofill"
            + ", t20 FLOAT(11,3) unsigned zerofill"
            + ", t21 FLOAT unsigned zerofill"
            + ", t22 DOUBLE(6,3) unsigned zerofill"
            + ", t23 DOUBLE(11,3) unsigned zerofill"
            + ", t24 DOUBLE unsigned zerofill");
    Statement stmt = sharedConnection.createStatement();
    stmt.executeUpdate(
        "insert into leadingZero values (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1.1,1.1,1.1,1.1,1.1,1.1,1.1,1.1,1.1), "
            + "(20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20.2,20.2,20.2,20.2,20.2,20.2,20.2,20.2,20.2)");

    // test text resultSet
    testLeadingZeroResult(stmt.executeQuery("select * from leadingZero"));

    // test binary resultSet
    if (!sharedUsePrepare()) {
      PreparedStatement pst1 = sharedConnection.prepareStatement("select * from leadingZero");
      ResultSet rs1 = pst1.executeQuery();
      testLeadingZeroResult(rs1);
    }
  }

  private void testLeadingZeroResult(ResultSet rs1) throws SQLException {
    assertTrue(rs1.next());
    assertEquals("001", rs1.getString(1));
    assertEquals("00000001", rs1.getString(2));
    assertEquals("001", rs1.getString(3));
    assertEquals("001", rs1.getString(4));
    assertEquals("00000001", rs1.getString(5));
    assertEquals("00001", rs1.getString(6));
    assertEquals("001", rs1.getString(7));
    assertEquals("00000001", rs1.getString(8));
    assertEquals("00000001", rs1.getString(9));
    assertEquals("001", rs1.getString(10));
    assertEquals("00000001", rs1.getString(11));
    assertEquals("0000000001", rs1.getString(12));
    assertEquals("001", rs1.getString(13));
    assertEquals("00000001", rs1.getString(14));
    assertEquals("00000000000000000001", rs1.getString(15));
    assertEquals("001.100", rs1.getString(16));
    assertEquals("00000001.100", rs1.getString(17));
    assertEquals("0000000001", rs1.getString(18));
    assertEquals("01.100", rs1.getString(19));
    assertEquals("0000001.100", rs1.getString(20));
    assertEquals("0000000001.1", rs1.getString(21));
    assertEquals("01.100", rs1.getString(22));
    assertEquals("0000001.100", rs1.getString(23));
    assertEquals("00000000000000000001.1", rs1.getString(24));

    assertTrue(rs1.next());
    assertEquals("020", rs1.getString(1));
    assertEquals("00000020", rs1.getString(2));
    assertEquals("020", rs1.getString(3));
    assertEquals("020", rs1.getString(4));
    assertEquals("00000020", rs1.getString(5));
    assertEquals("00020", rs1.getString(6));
    assertEquals("020", rs1.getString(7));
    assertEquals("00000020", rs1.getString(8));
    assertEquals("00000020", rs1.getString(9));
    assertEquals("020", rs1.getString(10));
    assertEquals("00000020", rs1.getString(11));
    assertEquals("0000000020", rs1.getString(12));
    assertEquals("020", rs1.getString(13));
    assertEquals("00000020", rs1.getString(14));
    assertEquals("00000000000000000020", rs1.getString(15));
    assertEquals("020.200", rs1.getString(16));
    assertEquals("00000020.200", rs1.getString(17));
    assertEquals("0000000020", rs1.getString(18));
    assertEquals("20.200", rs1.getString(19));
    assertEquals("0000020.200", rs1.getString(20));
    assertEquals("0000000020.2", rs1.getString(21));
    assertEquals("20.200", rs1.getString(22));
    assertEquals("0000020.200", rs1.getString(23));
    assertEquals("00000000000000000020.2", rs1.getString(24));
    assertFalse(rs1.next());
  }

  @Test
  public void firstForwardTest() throws SQLException {
    // first must always work when not streaming
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1");
    assertTrue(rs.first());
    assertFalse(rs.previous());
    assertTrue(rs.absolute(1));
    assertFalse(rs.relative(-1));

    // absolute operation must fail when streaming
    stmt.setFetchSize(1);
    rs = stmt.executeQuery("SELECT 1");
    try {
      rs.first();
      fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("Invalid operation for result set type TYPE_FORWARD_ONLY"));
    }
    try {
      rs.previous();
      fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("Invalid operation for result set type TYPE_FORWARD_ONLY"));
    }
    try {
      rs.absolute(1);
      fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("Invalid operation for result set type TYPE_FORWARD_ONLY"));
    }
    try {
      rs.relative(-1);
      fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("Invalid operation for result set type TYPE_FORWARD_ONLY"));
    }
  }

  /**
   * CONJ-429 : ResultSet.getDouble/getFloat may throws a NumberFormatException.
   *
   * @throws SQLException if any abnormal error occur
   */
  @Test
  public void testNumericType() throws SQLException {
    createTable(
        "numericTypeTable",
        "t1 tinyint, "
            + "t2 boolean, "
            + "t3 smallint,  "
            + "t4 mediumint, "
            + "t5 int, "
            + "t6 bigint, "
            + "t7 decimal, "
            + "t8 float, "
            + "t9 double, "
            + "t10 bit,"
            + "t11 char(10),"
            + "t12 varchar(10),"
            + "t13 binary(10),"
            + "t14 varbinary(10),"
            + "t15 text,"
            + "t16 blob,"
            + "t17 date");

    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute(
          "INSERT into numericTypeTable values (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 'a', 'a', 'a', 'a', 'a', 'a', now())");
      try (ResultSet rs = stmt.executeQuery("select * from numericTypeTable")) {
        assertTrue(rs.next());
        floatDoubleCheckResult(rs);
      }
    }
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("select * from numericTypeTable")) {
      try (ResultSet rs = preparedStatement.executeQuery()) {
        assertTrue(rs.next());
        floatDoubleCheckResult(rs);
      }
    }
  }

  private void floatDoubleCheckResult(ResultSet rs) throws SQLException {

    // getDouble
    // supported JDBC type :
    // TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, BOOLEAN,
    // CHAR, VARCHAR, LONGVARCHAR
    for (int i = 1; i < 11; i++) {
      rs.getDouble(i);
      rs.getFloat(i);
    }

    for (int i = 11; i < 16; i++) {
      try {
        rs.getDouble(i);
        fail();
      } catch (SQLException sqle) {
        assertTrue(sqle.getMessage().contains("Incorrect format "));
      }
      try {
        rs.getFloat(i);
        fail();
      } catch (SQLException sqle) {
        assertTrue(sqle.getMessage().contains("Incorrect format "));
      }
    }
    for (int i = 16; i < 18; i++) {
      try {
        rs.getDouble(i);
      } catch (SQLException sqle) {
        assertTrue(sqle.getMessage().contains("not available"));
      }
      try {
        rs.getFloat(i);
        fail();
      } catch (SQLException sqle) {
        assertTrue(sqle.getMessage().contains("not available"));
      }
    }
  }

  /**
   * CONJ-496 : Driver not dealing with non zero decimal values.
   *
   * @throws SQLException if any abnormal error occur
   */
  @Test
  public void numericTestWithDecimal() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 as test");
    assertTrue(rs.next());
    assertTrue(rs.getInt("test") == 1);
    assertTrue(rs.getByte("test") == 1);
    assertTrue(rs.getShort("test") == 1);

    rs = stmt.executeQuery("SELECT 1.3333 as test");
    assertTrue(rs.next());
    assertTrue(rs.getInt("test") == 1);
    assertTrue(rs.getByte("test") == 1);
    assertTrue(rs.getShort("test") == 1);
    assertTrue(rs.getLong("test") == 1);
    assertTrue(rs.getFloat("test") == 1.3333F);

    rs = stmt.executeQuery("SELECT 1.0 as test");
    assertTrue(rs.next());
    assertTrue(rs.getInt("test") == 1);
    assertTrue(rs.getByte("test") == 1);
    assertTrue(rs.getShort("test") == 1);
    assertTrue(rs.getLong("test") == 1);
    assertTrue(rs.getFloat("test") == 1.0F);

    rs = stmt.executeQuery("SELECT -1 as test");
    assertTrue(rs.next());
    assertTrue(rs.getInt("test") == -1);
    assertTrue(rs.getByte("test") == -1);
    assertTrue(rs.getShort("test") == -1);
    assertTrue(rs.getLong("test") == -1);

    rs = stmt.executeQuery("SELECT -1.0 as test");
    assertTrue(rs.next());
    assertTrue(rs.getInt("test") == -1);
    assertTrue(rs.getByte("test") == -1);
    assertTrue(rs.getShort("test") == -1);
    assertTrue(rs.getLong("test") == -1);
    assertTrue(rs.getFloat("test") == -1.0F);

    rs = stmt.executeQuery("SELECT -1.3333 as test");
    assertTrue(rs.next());
    assertTrue(rs.getInt("test") == -1);
    assertTrue(rs.getByte("test") == -1);
    assertTrue(rs.getShort("test") == -1);
    assertTrue(rs.getLong("test") == -1);
    assertTrue(rs.getFloat("test") == -1.3333F);
  }

  @Test
  public void nullField() throws SQLException {
    Assume.assumeTrue(isMariadbServer()); // '0000-00-00' doesn't work anymore on mysql 5.7.
    createTable("nullField", "t1 varchar(50), t2 timestamp NULL, t3 date, t4 year(4)");
    Statement stmt = sharedConnection.createStatement();
    stmt.execute(
        "INSERT INTO nullField(t1,t2,t3,t4) values "
            + "(null, '0000-00-00 00:00:00', '0000-00-00', '0000'), "
            + "(null, null, null, null),"
            + "('aa', now(), now(), '2017')");

    ResultSet rs = stmt.executeQuery("SELECT * FROM nullField");
    assertTrue(rs.next());
    if (!sharedOptions().useServerPrepStmts) {
      assertNull(rs.getString(1));
      assertTrue(rs.wasNull());

      assertNull(rs.getTimestamp(2));
      assertTrue(rs.wasNull());

      assertEquals("0000-00-00 00:00:00", rs.getString(2));
      assertFalse(rs.wasNull());

      assertNull(rs.getDate(3));
      assertTrue(rs.wasNull());

      assertEquals("0000-00-00", rs.getString(3));
      assertFalse(rs.wasNull());

      assertEquals(Date.valueOf("0000-01-01"), rs.getDate(4));
      assertFalse(rs.wasNull());

      assertEquals(0, rs.getInt(4));
      assertFalse(rs.wasNull());

      assertEquals("0001-01-01", rs.getString(4));
      assertFalse(rs.wasNull());
    }

    assertTrue(rs.next());

    assertNull(rs.getTimestamp(2));
    assertTrue(rs.wasNull());

    assertNull(rs.getString(2));
    assertTrue(rs.wasNull());

    assertNull(rs.getDate(3));
    assertTrue(rs.wasNull());

    assertNull(rs.getString(3));
    assertTrue(rs.wasNull());

    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());

    assertNull(rs.getString(2));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());

    assertNotNull(rs.getTimestamp(2));
    assertFalse(rs.wasNull());

    assertNotNull(rs.getString(2));
    assertFalse(rs.wasNull());

    assertNotNull(rs.getDate(3));
    assertFalse(rs.wasNull());

    assertNotNull(rs.getString(3));
    assertFalse(rs.wasNull());

    assertNotNull(rs.getDate(4));
    assertFalse(rs.wasNull());

    assertNotNull(rs.getString(2));
    assertFalse(rs.wasNull());
  }

  @Test
  public void doubleStringResults() throws SQLException {
    createTable("doubleStringResults", "i double, j float");
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("INSERT INTO doubleStringResults VALUES (1.1, 1.2), (23, 24)");
    ResultSet rs = stmt.executeQuery("SELECT * FROM doubleStringResults");

    assertTrue(rs.next());
    assertEquals("1.1", rs.getString(1));
    assertEquals(1, rs.getInt(1));
    assertEquals(1.1, rs.getDouble(1), 0.0001);
    assertEquals("1.2", rs.getString(2));
    assertEquals(1, rs.getInt(2));
    assertEquals(1.2, rs.getFloat(2), 0.0001);

    assertTrue(rs.next());
    assertEquals("23", rs.getString(1));
    assertEquals(23, rs.getInt(1));
    assertEquals(23, rs.getDouble(1), 0.0001);
    assertEquals("24", rs.getString(2));
    assertEquals(24, rs.getInt(2));
    assertEquals(24, rs.getFloat(2), 0.0001);
  }

  @Test
  public void invisibleColumn() throws SQLException {

    // since 10.3.3
    Assume.assumeTrue(isMariadbServer() && minVersion(10, 3));
    cancelForVersion(10, 3, 0);
    cancelForVersion(10, 3, 1);
    cancelForVersion(10, 3, 2);

    createTable("invisible", "x INT, y INT INVISIBLE, z INT INVISIBLE NOT NULL DEFAULT 4");
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("INSERT INTO invisible(x,y) VALUES (1,2)");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT * FROM invisible WHERE x = ?")) {
      ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertEquals("x", resultSetMetaData.getColumnName(1));
    }

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT x,z FROM invisible WHERE x = ?")) {
      ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData();
      Assert.assertEquals(2, resultSetMetaData.getColumnCount());
      Assert.assertEquals("x", resultSetMetaData.getColumnName(1));
      Assert.assertEquals("z", resultSetMetaData.getColumnName(2));
    }
  }

  @Test
  public void checkInvisibleMetaData() throws SQLException {

    // since 10.3.3
    Assume.assumeTrue(isMariadbServer() && minVersion(10, 3));
    cancelForVersion(10, 3, 0);
    cancelForVersion(10, 3, 1);
    cancelForVersion(10, 3, 2);

    createTable(
        "checkInvisibleMetaData",
        "xx tinyint(1), x2 tinyint(1) unsigned INVISIBLE primary key auto_increment, yy year(4), zz bit, uu smallint");
    DatabaseMetaData meta = sharedConnection.getMetaData();
    ResultSet rs = meta.getColumns(null, null, "checkInvisibleMetaData", null);
    assertTrue(rs.next());
    assertEquals("BIT", rs.getString(6));
    assertTrue(rs.next());
    assertEquals("BIT", rs.getString(6));
    assertTrue(rs.next());
    assertEquals("YEAR", rs.getString(6));
    assertEquals(null, rs.getString(7)); // column size
    assertEquals(null, rs.getString(9)); // decimal digit
  }

  @Test
  public void columnNamesMappingError() throws SQLException {
    createTable("columnNamesMappingError", "xx tinyint(1)");
    Statement stmt = sharedConnection.createStatement();
    stmt.executeUpdate("INSERT INTO columnNamesMappingError VALUES (4)");
    ResultSet rs = stmt.executeQuery("SELECT * FROM columnNamesMappingError");
    assertTrue(rs.next());
    assertEquals(4, rs.getInt("xx"));
    try {
      rs.getInt("wrong_column_name");
      fail("must have fail, column 'wrong_column_name' does not exists");
    } catch (SQLException e) {
      assertEquals("42S22", e.getSQLState());
      assertEquals(1054, e.getErrorCode());
      assertEquals("No such column: wrong_column_name", e.getMessage());
    }
  }

  /**
   * CONJ-669: No such column when having empty column without alias.
   *
   * @throws SQLException exception
   */
  @Test
  public void emptyColumnName() throws SQLException {
    try (Connection connection = setConnection("")) {
      Statement stmt = connection.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE emptyColumn (id int)");
      stmt.execute("INSERT INTO emptyColumn value (1)");
      ResultSet rs = stmt.executeQuery("SELECT '' FROM emptyColumn");
      while (rs.next()) {
        Assert.assertEquals("", rs.getString(1));
        Assert.assertEquals("", rs.getString(""));
      }
      ResultSetMetaData meta = rs.getMetaData();
      Assert.assertEquals(1, meta.getColumnCount());
      Assert.assertEquals("", meta.getColumnName(1));
    }
  }
}
