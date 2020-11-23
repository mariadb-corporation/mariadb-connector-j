/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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

import static org.junit.Assert.*;

import java.sql.*;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class GeneratedKeysTest extends BaseTest {

  @BeforeClass()
  public static void initClass() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute(
          "CREATE TABLE gen_key_test(id INTEGER NOT NULL AUTO_INCREMENT, name VARCHAR(100), PRIMARY KEY (id))");
      stmt.execute(
          "CREATE TABLE gen_key_test2(id INTEGER NOT NULL AUTO_INCREMENT, name VARCHAR(100), PRIMARY KEY (id))");
      stmt.execute("FLUSH TABLES");
    }
  }

  @AfterClass
  public static void afterClass() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS gen_key_test");
      stmt.execute("DROP TABLE IF EXISTS gen_key_test2");
    }
  }

  @Test
  public void testSimpleGeneratedKeys() throws SQLException {
    Statement statement = sharedConnection.createStatement();
    statement.execute("truncate gen_key_test");
    statement.executeUpdate(
        "INSERT INTO gen_key_test (id, name) VALUES (null, 'Dave')",
        Statement.RETURN_GENERATED_KEYS);
    int[] autoInc = setAutoInc();
    ResultSet resultSet = statement.getGeneratedKeys();
    assertTrue(resultSet.next());
    assertEquals(autoInc[0] + autoInc[1], resultSet.getInt(1));
  }

  @Test
  public void testSimpleGeneratedKeysWithPreparedStatement() throws SQLException {
    sharedConnection.createStatement().execute("truncate gen_key_test");
    PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "INSERT INTO gen_key_test (id, name) VALUES (null, ?)",
            Statement.RETURN_GENERATED_KEYS);

    preparedStatement.setString(1, "Dave");
    preparedStatement.execute();

    ResultSet resultSet = preparedStatement.getGeneratedKeys();
    assertTrue(resultSet.next());
    int[] autoInc = setAutoInc();
    assertEquals(autoInc[0] + autoInc[1], resultSet.getInt(1));
  }

  @Test
  public void testGeneratedKeysInsertOnDuplicateUpdate() throws SQLException {
    Statement statement = sharedConnection.createStatement();
    statement.execute("truncate gen_key_test");
    statement.execute("INSERT INTO gen_key_test (name) VALUES ('Dave')");
    int[] autoInc = setAutoInc();
    statement.executeUpdate(
        "INSERT INTO gen_key_test (id, name) VALUES ("
            + (autoInc[0] + autoInc[1])
            + ", 'Dave') ON DUPLICATE KEY UPDATE id = id",
        Statement.RETURN_GENERATED_KEYS);
    // From the Javadoc: "If this Statement object did not generate any keys, an empty ResultSet
    // object is returned."
    ResultSet resultSet = statement.getGeneratedKeys();
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals(1, resultSetMetaData.getColumnCount());
    // Since the statement does not generate any keys an empty ResultSet should be returned
    assertFalse(resultSet.next());
  }

  /**
   * CONJ-284: Cannot read autoincremented IDs bigger than Short.MAX_VALUE.
   *
   * @throws SQLException exception
   */
  @Test
  public void testGeneratedKeysNegativeValue() throws SQLException {
    Assume.assumeFalse(isGalera());
    Statement statement = sharedConnection.createStatement();
    statement.execute("ALTER TABLE gen_key_test2 AUTO_INCREMENT = 65500");
    PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "INSERT INTO gen_key_test2 (name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setString(1, "t");
    preparedStatement.execute();

    ResultSet rs = preparedStatement.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(65500, rs.getInt(1));
  }
}
