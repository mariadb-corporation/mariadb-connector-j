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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;

public class AllowMultiQueriesTest extends BaseTest {

  /**
   * Tables initialisation.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable(
        "AllowMultiQueriesTest", "id int not null primary key auto_increment, test varchar(10)");
    createTable(
        "AllowMultiQueriesTest2", "id int not null primary key auto_increment, test varchar(10)");
    if (testSingleHost) {
      try (Statement stmt = sharedConnection.createStatement()) {
        stmt.execute("INSERT INTO AllowMultiQueriesTest(test) VALUES ('a'), ('b')");
      }
    }
  }

  @Test
  public void allowMultiQueriesSingleTest() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
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
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute("SELECT 1; SET @TOTO=3; SELECT 2", Statement.RETURN_GENERATED_KEYS);
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
  public void allowMultiQueriesFetchTest() {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      try (Statement statement = connection.createStatement()) {
        statement.setFetchSize(1);
        statement.execute(
            "SELECT * from AllowMultiQueriesTest;SELECT * from AllowMultiQueriesTest;");
        do {
          ResultSet resultSet = statement.getResultSet();
          assertEquals(-1, statement.getUpdateCount());
          assertTrue(resultSet.next());
          assertEquals("a", resultSet.getString(2));
        } while (statement.getMoreResults());
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void allowMultiQueriesFetchKeepTest() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      try (Statement statement = connection.createStatement()) {
        statement.setFetchSize(1);
        statement.execute("SELECT * from AllowMultiQueriesTest;SELECT 3;");
        ResultSet rs1 = statement.getResultSet();
        assertTrue(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertTrue(rs1.next());
        assertEquals("a", rs1.getString(2));

        ResultSet rs = statement.getResultSet();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
      }
    }
  }

  @Test
  public void allowMultiQueriesFetchCloseTest() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
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
        assertTrue(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertTrue(rs1.next());
        assertEquals("a", rs1.getString(2));

        ResultSet rs = statement.getResultSet();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
      }
    }
  }

  @Test
  public void allowMultiQueriesFetchInsertSelectTest() throws SQLException {
    try (Connection connection = setConnection("&allowMultiQueries=true")) {
      try (Statement statement = connection.createStatement()) {
        statement.setFetchSize(1);
        statement.execute(
            "INSERT INTO AllowMultiQueriesTest2(test) VALUES ('a'), ('b');SELECT * from AllowMultiQueriesTest;SELECT 3;");
      }
    }
  }
}
