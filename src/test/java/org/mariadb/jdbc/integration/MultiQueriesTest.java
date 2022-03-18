// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class MultiQueriesTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    after2();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE AllowMultiQueriesTest(id int not null primary key auto_increment, "
            + "test varchar(10))");
    stmt.execute("INSERT INTO AllowMultiQueriesTest(test) VALUES ('a'), ('b')");
  }

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IF EXISTS AllowMultiQueriesTest");
  }

  @Test
  public void allowMultiQueriesSingleTest() throws SQLException {
    try (Connection connection = createCon("&allowMultiQueries=true")) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1; SELECT 2; SELECT 3;");
        int counter = 1;
        do {
          ResultSet resultSet = statement.getResultSet();
          assertEquals(-1, statement.getUpdateCount());
          assertTrue(resultSet.next());
          assertEquals(counter++, resultSet.getInt(1));
        } while (statement.getMoreResults());
        assertEquals(4, counter);
      }
    }
  }

  @Test
  public void checkMultiGeneratedKeys() throws SQLException {
    try (Connection connection = createCon("&allowMultiQueries=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute("SELECT 1; SET @TOTO=3; SELECT 2", java.sql.Statement.RETURN_GENERATED_KEYS);
      ResultSet rs = stmt.getResultSet();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertFalse(stmt.getMoreResults());
      stmt.getGeneratedKeys();
      assertTrue(stmt.getMoreResults());
      rs = stmt.getResultSet();
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }
  }

  @Test
  public void allowMultiQueriesFetchTest() throws SQLException {
    try (Connection connection = createCon("&allowMultiQueries=true")) {
      try (Statement stmt = connection.createStatement()) {
        stmt.setFetchSize(1);
        stmt.execute("SELECT * from AllowMultiQueriesTest;SELECT * from AllowMultiQueriesTest;");
        do {
          ResultSet resultSet = stmt.getResultSet();
          assertEquals(-1, stmt.getUpdateCount());
          assertTrue(resultSet.next());
          assertEquals("a", resultSet.getString(2));
        } while (stmt.getMoreResults());

        stmt.executeQuery(
            "SELECT * from AllowMultiQueriesTest;SELECT * from AllowMultiQueriesTest;");
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
      }
    }
  }

  @Test
  public void quitWhileStreaming() throws SQLException {
    // XPAND doesn't support DO command
    Assumptions.assumeFalse(isXpand());

    Connection connection = createCon("&allowMultiQueries=true");
    Statement stmt = connection.createStatement();
    stmt.setFetchSize(1);
    stmt.executeQuery(
        "DO 2;SELECT * from AllowMultiQueriesTest;SELECT * from AllowMultiQueriesTest; DO 1; SELECT 2");
    connection.abort(Runnable::run);

    connection = createCon("&allowMultiQueries=true");
    stmt = connection.createStatement();
    stmt.setFetchSize(1);
    stmt.executeQuery("DO 2;DO 1;SELECT * from AllowMultiQueriesTest");
    connection.abort(Runnable::run);
  }

  @Test
  public void allowMultiQueriesFetchKeepTest() throws SQLException {
    try (Connection connection = createCon("&allowMultiQueries=true")) {
      try (Statement stmt = connection.createStatement()) {
        stmt.setFetchSize(1);
        stmt.execute("SELECT * from AllowMultiQueriesTest;SELECT 3;");
        ResultSet rs1 = stmt.getResultSet();
        assertTrue(stmt.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
        assertTrue(rs1.next());
        assertEquals("a", rs1.getString(2));

        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
      }
    }
  }

  @Test
  public void allowMultiQueriesFetchCloseTest() throws SQLException {
    try (Connection connection = createCon("&allowMultiQueries=true")) {
      try (Statement statement = connection.createStatement()) {
        statement.setFetchSize(1);
        statement.execute(
            "SELECT * from AllowMultiQueriesTest;SELECT * from AllowMultiQueriesTest;SELECT 3;");
        ResultSet rs1 = statement.getResultSet();
        assertTrue(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        try {
          rs1.next();
          fail("Must have thrown exception, since closed");
        } catch (SQLException sqle) {
          assertTrue(sqle.getMessage().contains("Operation not permit on a closed resultSet"));
        }

        rs1 = statement.getResultSet();
        assertTrue(statement.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
        assertTrue(rs1.next());
        assertEquals("a", rs1.getString(2));

        ResultSet rs = statement.getResultSet();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
      }
    }
  }
}
