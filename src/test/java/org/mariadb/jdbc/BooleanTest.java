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
import org.junit.BeforeClass;
import org.junit.Test;

public class BooleanTest extends BaseTest {

  /**
   * Initialization.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("booleantest", "id int not null primary key auto_increment, test boolean");
    createTable("booleanvalue", "test boolean");
    createTable(
        "booleanAllField",
        "t1 BIT, t2 TINYINT(1), t3 SMALLINT(1), t4 MEDIUMINT(1), t5 INT(1), t6 BIGINT(1), t7 DECIMAL(1), t8 FLOAT, "
            + "t9 DOUBLE, t10 CHAR(1), t11 VARCHAR(1), t12 BINARY(1), t13 BLOB(1), t14 TEXT(1)");
  }

  @Test
  public void testBoolean() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("insert into booleantest values(null, true)");
    stmt.execute("insert into booleantest values(null, false)");
    ResultSet rs = stmt.executeQuery("select * from booleantest");
    if (rs.next()) {
      assertTrue(rs.getBoolean(2));
      if (rs.next()) {
        assertFalse(rs.getBoolean(2));
      } else {
        fail("must have a result !");
      }
    } else {
      fail("must have a result !");
    }
  }

  @Test
  public void testBooleanSet() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("CREATE TEMPORARY TABLE testBooleanSet (test BOOLEAN)");
    try (PreparedStatement prep =
        sharedConnection.prepareStatement("INSERT INTO testBooleanSet VALUE (?)")) {
      prep.setBoolean(1, true);
      prep.execute();
      prep.setBoolean(1, false);
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("select * from testBooleanSet");
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
    assertTrue(rs.next());
    assertFalse(rs.getBoolean(1));
  }

  @Test
  public void testBooleanString() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("insert into booleanvalue values(true)");
    stmt.execute("insert into booleanvalue values(false)");
    stmt.execute("insert into booleanvalue values(4)");
    ResultSet rs = stmt.executeQuery("select * from booleanvalue");

    if (rs.next()) {
      assertTrue(rs.getBoolean(1));
      assertEquals("1", rs.getString(1));
      if (rs.next()) {
        assertFalse(rs.getBoolean(1));
        assertEquals("0", rs.getString(1));
        if (rs.next()) {
          assertTrue(rs.getBoolean(1));
          assertEquals("4", rs.getString(1));
        } else {
          fail("must have a result !");
        }
      } else {
        fail("must have a result !");
      }
    } else {
      fail("must have a result !");
    }
  }

  /**
   * CONJ-254 error when using scala anorm string interpolation.
   *
   * @throws Exception exception
   */
  @Test
  public void testBooleanAllField() throws Exception {
    try (Connection connection = setConnection("&maxPerformance=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO booleanAllField VALUES (null, null, null, null, null, null, null, null, null, null, null, null, null, null)");
      stmt.execute(
          "INSERT INTO booleanAllField VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, '0', '0', '0', '0', '0')");
      stmt.execute(
          "INSERT INTO booleanAllField VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, '1', '1', '1', '1', '1')");
      stmt.execute(
          "INSERT INTO booleanAllField VALUES (1, 2, 2, 2, 2, 2, 2, 2, 2, '2', '2', '2', '2', '2')");

      ResultSet rs = stmt.executeQuery("SELECT * FROM booleanAllField");
      checkBooleanValue(rs, false, null);
      checkBooleanValue(rs, false, false);
      checkBooleanValue(rs, true, true);
      checkBooleanValue(rs, true, true);

      PreparedStatement preparedStatement =
          connection.prepareStatement("SELECT * FROM booleanAllField WHERE 1 = ?");
      preparedStatement.setInt(1, 1);
      rs = preparedStatement.executeQuery();
      checkBooleanValue(rs, false, null);
      checkBooleanValue(rs, false, false);
      checkBooleanValue(rs, true, true);
      checkBooleanValue(rs, true, true);
    }
  }

  private void checkBooleanValue(ResultSet rs, boolean expectedValue, Boolean expectedNull)
      throws SQLException {
    assertTrue(rs.next());
    for (int i = 1; i <= 14; i++) {
      assertEquals(expectedValue, rs.getBoolean(i));
      if (i == 1 || i == 2) {
        assertEquals(expectedNull, rs.getObject(i));
        assertEquals(expectedNull, rs.getObject(i, Boolean.class));
      }
    }
  }
}
