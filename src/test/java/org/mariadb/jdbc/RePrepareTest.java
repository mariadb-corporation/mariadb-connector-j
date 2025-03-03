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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class RePrepareTest extends BaseTest {

  @BeforeClass()
  public static void initClass() throws SQLException {
    drop();
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("CREATE TABLE rePrepareTestSelectError(test int)");
      stmt.execute("CREATE TABLE rePrepareTestInsertError(test int)");
      stmt.execute("CREATE TABLE cannotRePrepare(test int)");
      stmt.execute("FLUSH TABLES");
    }
  }

  @AfterClass
  public static void drop() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS rePrepareTestSelectError");
      stmt.execute("DROP TABLE IF EXISTS rePrepareTestInsertError");
      stmt.execute("DROP TABLE IF EXISTS cannotRePrepare");
    }
  }

  @Test
  public void rePrepareTestSelectError() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("INSERT INTO rePrepareTestSelectError(test) VALUES (1)");
      try (PreparedStatement preparedStatement =
          sharedConnection.prepareStatement(
              "SELECT * FROM rePrepareTestSelectError where test = ?")) {
        preparedStatement.setInt(1, 1);
        ResultSet rs = preparedStatement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        stmt.execute(
            "ALTER TABLE rePrepareTestSelectError"
                + " CHANGE COLUMN `test` `test` VARCHAR(50) NULL DEFAULT NULL FIRST,"
                + "ADD COLUMN `test2` VARCHAR(50) NULL DEFAULT NULL AFTER `test`;");
        ResultSet rs2 = preparedStatement.executeQuery();
        preparedStatement.setInt(1, 1);
        assertTrue(rs2.next());
        assertEquals("1", rs2.getString(1));
        assertFalse(rs2.next());
      }
    }
  }

  @Test
  public void rePrepareTestInsertError() throws SQLException {
    Assume.assumeFalse(sharedIsAurora()); // Aurora has not "flush tables with read lock" right;
    Assume.assumeFalse(!isMariadbServer() && minVersion(8, 0, 0)); // froze when flush
    try (Statement stmt = sharedConnection.createStatement()) {
      try (PreparedStatement preparedStatement =
          sharedConnection.prepareStatement(
              "INSERT INTO rePrepareTestInsertError(test) values (?)")) {

        preparedStatement.setInt(1, 1);
        preparedStatement.execute();

        stmt.execute(
            "ALTER TABLE rePrepareTestInsertError"
                + " CHANGE COLUMN `test` `test` VARCHAR(50) NULL DEFAULT NULL FIRST;");

        preparedStatement.setInt(1, 2);
        preparedStatement.execute();

        stmt.execute(
            "ALTER TABLE rePrepareTestInsertError"
                + " CHANGE COLUMN `test` `test` VARCHAR(100) NULL DEFAULT NULL FIRST,"
                + "ADD COLUMN `test2` VARCHAR(50) NULL DEFAULT NULL AFTER `test`;");

        stmt.execute("flush tables with read lock");
        stmt.execute("unlock tables");
        preparedStatement.setInt(1, 3);
        preparedStatement.execute();
      }
    }
  }

  @Test
  public void cannotRePrepare() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      try (PreparedStatement preparedStatement =
          sharedConnection.prepareStatement("INSERT INTO cannotRePrepare(test) values (?)")) {

        preparedStatement.setInt(1, 1);
        preparedStatement.execute();

        stmt.execute(
            "ALTER TABLE cannotRePrepare"
                + " CHANGE COLUMN `test` `otherName` VARCHAR(50) NULL DEFAULT NULL FIRST;");

        preparedStatement.setInt(1, 2);
        try {
          preparedStatement.execute();
          fail();
        } catch (SQLException sqle) {
          assertTrue(sqle.getMessage(), sqle.getMessage().contains("Unknown column 'test' in "));
        }
      }
    }
  }
}
