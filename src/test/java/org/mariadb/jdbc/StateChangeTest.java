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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Assume;
import org.junit.Test;

public class StateChangeTest extends BaseTest {

  @Test
  public void databaseStateChange() throws SQLException {
    Assume.assumeTrue(
        (isMariadbServer() && minVersion(10, 2)) || (!isMariadbServer() && minVersion(5, 7)));
    try (Connection connection = setConnection()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("drop database if exists " + MariaDbConnection.quoteIdentifier("_test_db"));
        stmt.execute("create database " + MariaDbConnection.quoteIdentifier("_test_db"));
        assertEquals(database, connection.getCatalog());
        stmt.execute("USE " + MariaDbConnection.quoteIdentifier("_test_db"));
        assertEquals("_test_db", connection.getCatalog());
      }
    }
  }

  @Test
  public void timeZoneChange() throws SQLException {
    Assume.assumeTrue(
        (isMariadbServer() && minVersion(10, 2)) || (!isMariadbServer() && minVersion(5, 7)));
    try (Connection connection = setConnection()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("drop database if exists " + MariaDbConnection.quoteIdentifier("_test_db"));
        stmt.execute("create database " + MariaDbConnection.quoteIdentifier("_test_db"));
        assertEquals(database, connection.getCatalog());
        stmt.execute("USE " + MariaDbConnection.quoteIdentifier("_test_db"));
        assertEquals("_test_db", connection.getCatalog());
      }
    }
  }

  @Test
  public void autocommitChange() throws SQLException {
    try (Connection connection = setConnection()) {
      try (Statement stmt = connection.createStatement()) {
        assertTrue(connection.getAutoCommit());
        stmt.execute("SET autocommit=false");
        assertFalse(connection.getAutoCommit());
      }
    }
  }

  @Test
  public void autoIncrementChange() throws SQLException {
    Assume.assumeFalse(isGalera());
    Assume.assumeTrue(
        (isMariadbServer() && minVersion(10, 2)) || (!isMariadbServer() && minVersion(5, 7)));
    createTable("autoIncrementChange", "id int not null primary key auto_increment, name char(20)");
    try (Connection connection = setConnection()) {
      try (Statement stmt = connection.createStatement()) {
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "INSERT INTO autoIncrementChange(name) value (?)",
                Statement.RETURN_GENERATED_KEYS)) {

          preparedStatement.setString(1, "a");
          preparedStatement.execute();
          ResultSet rs = preparedStatement.getGeneratedKeys();
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));

          preparedStatement.setString(1, "b");
          preparedStatement.execute();
          rs = preparedStatement.getGeneratedKeys();
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));

          stmt.execute("SET @@session.auto_increment_increment=10");
          ResultSet rs2 =
              stmt.executeQuery(
                  "SHOW VARIABLES WHERE Variable_name like 'auto_increment_increment'");
          assertTrue(rs2.next());
          assertEquals(10, rs2.getInt(2));

          preparedStatement.setString(1, "c");
          preparedStatement.execute();
          rs = preparedStatement.getGeneratedKeys();
          assertTrue(rs.next());
          assertEquals(11, rs.getInt(1));

          rs2 = stmt.executeQuery("select * from autoIncrementChange");
          assertTrue(rs2.next());
          assertEquals("a", rs2.getString(2));
          assertEquals(1, rs2.getInt(1));
          assertTrue(rs2.next());
          assertEquals("b", rs2.getString(2));
          assertEquals(2, rs2.getInt(1));
          assertTrue(rs2.next());
          assertEquals("c", rs2.getString(2));
          assertEquals(11, rs2.getInt(1));
          assertFalse(rs2.next());
        }
      }
    }
  }
}
