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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ScrollTypeTest extends BaseTest {

  @BeforeClass()
  public static void initClass() throws SQLException {
    afterClass();
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute(
          "CREATE TABLE resultsSetReadingTest(id int not null primary key auto_increment, test int)");
      stmt.execute("CREATE TABLE scrollMultipleFetch(intvalue int)");
      stmt.execute("FLUSH TABLES");
      if (testSingleHost) {
        stmt.execute("INSERT INTO resultsSetReadingTest (test) values (1), (2), (3)");
      }
    }
  }

  @AfterClass
  public static void afterClass() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS resultsSetReadingTest");
      stmt.execute("DROP TABLE IF EXISTS scrollMultipleFetch");
    }
  }

  @Test
  public void scrollInsensitivePrepareStmt() throws SQLException {
    try (PreparedStatement stmt =
        sharedConnection.prepareStatement(
            "SELECT * FROM resultsSetReadingTest",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      stmt.setFetchSize(2);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.beforeFirst();
      } catch (SQLException sqle) {
        fail("beforeFirst() should work on a TYPE_SCROLL_INSENSITIVE result set");
      }
    }
  }

  @Test
  public void scrollInsensitiveStmt() throws SQLException {
    try (Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      stmt.setFetchSize(2);
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM resultsSetReadingTest")) {
        rs.beforeFirst();
      } catch (SQLException sqle) {
        fail("beforeFirst() should work on a TYPE_SCROLL_INSENSITIVE result set");
      }
    }
  }

  @Test(expected = SQLException.class)
  public void scrollForwardOnlyPrepareStmt() throws SQLException {
    Assume.assumeFalse(sharedIsRewrite());
    try (PreparedStatement stmt =
        sharedConnection.prepareStatement(
            "SELECT * FROM resultsSetReadingTest",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY)) {
      stmt.setFetchSize(2);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.beforeFirst();
        fail("beforeFirst() shouldn't work on a TYPE_FORWARD_ONLY result set");
      }
    }
  }

  @Test(expected = SQLException.class)
  public void scrollForwardOnlyStmt() throws SQLException {
    try (Statement stmt =
        sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      stmt.setFetchSize(2);
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM resultsSetReadingTest")) {
        rs.beforeFirst();
        fail("beforeFirst() shouldn't work on a TYPE_FORWARD_ONLY result set");
      }
    }
  }

  @Test
  public void scrollMultipleFetch() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("INSERT INTO scrollMultipleFetch values (1), (2), (3)");
    stmt.setFetchSize(1);
    ResultSet rs = stmt.executeQuery("Select * from scrollMultipleFetch");
    rs.next();
    // don't read result-set fully
    assertEquals(1, rs.getFetchSize());

    ResultSet rs2 = stmt.executeQuery("Select * from scrollMultipleFetch");
    assertEquals(1, rs2.getFetchSize());
    assertEquals(1, rs.getFetchSize());
  }
}
