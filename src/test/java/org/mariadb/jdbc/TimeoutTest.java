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
import org.junit.Assume;
import org.junit.Test;

public class TimeoutTest extends BaseTest {

  private static int selectValue(Connection conn, int value) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("select " + value)) {
        if (rs.next()) {
          return rs.getInt(1);
        } else {
          return value;
        }
      }
    }
  }

  /** Conj-79. */
  @Test
  public void resultSetAfterSocketTimeoutTest() {
    // appveyor vm are very slow, cannot compare time
    Assume.assumeTrue(System.getenv("APPVEYOR") == null && System.getenv("DOCKER_SOCKET") == null);

    Assume.assumeFalse(sharedIsAurora());
    int went = 0;
    for (int j = 0; j < 100; j++) {
      try (Connection connection = setConnection("&connectTimeout=5&socketTimeout=1")) {
        boolean bugReproduced = false;

        int repetition = 1000;
        for (int i = 0; i < repetition && !connection.isClosed(); i++) {
          try {
            int v1 = selectValue(connection, 1);
            int v2 = selectValue(connection, 2);
            if (v1 != 1 || v2 != 2) {
              bugReproduced = true;
              break;
            }
            went++;
          } catch (SQLNonTransientConnectionException e) {
            // error due to socketTimeout
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
        assertFalse(bugReproduced); // either Exception or fine
      } catch (SQLException e) {
        // SQLNonTransientConnectionException error
      }
    }
    assertTrue(went > 0);
  }

  /**
   * Conj-79.
   *
   * @throws SQLException exception
   */
  @Test
  public void socketTimeoutTest() throws SQLException {
    Assume.assumeFalse(sharedIsAurora());
    // set a short connection timeout
    try (Connection connection = setConnection("&connectTimeout=1000&socketTimeout=1000")) {
      PreparedStatement ps = connection.prepareStatement("SELECT 1");
      ResultSet rs = ps.executeQuery();
      assertTrue(rs.next());
      logInfo(rs.getString(1));

      // wait for the connection to time out
      ps = connection.prepareStatement("SELECT sleep(2)");

      // a timeout should occur here
      try {
        ps.executeQuery();
        fail();
      } catch (SQLException e) {
        // check that it's a timeout that occurs
      }

      try {
        ps = connection.prepareStatement("SELECT 2");
        ps.execute();
        fail("Connection must have thrown error");
      } catch (SQLException e) {
        // normal exception
      }

      // the connection should  be closed
      assertTrue(connection.isClosed());
    }
  }

  @Test
  public void waitTimeoutStatementTest() throws SQLException, InterruptedException {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("set session wait_timeout=1");
        Thread.sleep(2000); // Wait for the server to kill the connection

        logInfo(connection.toString());

        // here a SQLNonTransientConnectionException is expected
        // "Could not read resultset: unexpected end of stream, ..."
        try {
          statement.execute("SELECT 1");
          fail("Connection must have thrown error");
        } catch (SQLException e) {
          // normal exception
        }
      }
    }
  }

  @Test
  public void waitTimeoutResultSetTest() throws SQLException, InterruptedException {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection()) {
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT 1");

      assertTrue(rs.next());

      stmt.execute("set session wait_timeout=1");
      Thread.sleep(3000); // Wait for the server to kill the connection

      // here a SQLNonTransientConnectionException is expected
      // "Could not read resultset: unexpected end of stream, ..."
      try {
        rs = stmt.executeQuery("SELECT 2");
        assertTrue(rs.next());
        fail("Connection must have thrown error");
      } catch (SQLException e) {
        // normal exception
      }
    }
  }
}
