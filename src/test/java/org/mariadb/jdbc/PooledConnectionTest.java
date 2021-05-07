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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import org.junit.Assume;
import org.junit.Test;

public class PooledConnectionTest extends BaseTest {

  @Test(expected = SQLException.class)
  public void testPooledConnectionClosed() throws Exception {
    ConnectionPoolDataSource ds =
        new MariaDbDataSource(hostname != null ? hostname : "localhost", port, database);
    PooledConnection pc = ds.getPooledConnection(username, password);
    Connection connection = pc.getConnection();
    MyEventListener listener = new MyEventListener();
    pc.addConnectionEventListener(listener);
    pc.addStatementEventListener(listener);
    connection.close();
    assertTrue(listener.closed);
    /* Verify physical connection is still ok */
    connection.createStatement().execute("select 1");

    /* close physical connection */
    pc.close();
    /* Now verify physical connection is gone */
    connection.createStatement().execute("select 1");
    fail("should never get there : previous must have thrown exception");
  }

  @Test(expected = SQLException.class)
  public void testPooledConnectionException() throws Exception {
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

    ConnectionPoolDataSource ds =
        new MariaDbDataSource(hostname != null ? hostname : "localhost", port, database);
    PooledConnection pc = null;
    try {
      pc = ds.getPooledConnection(username, password);
      MyEventListener listener = new MyEventListener();
      pc.addConnectionEventListener(listener);
      MariaDbConnection connection = (MariaDbConnection) pc.getConnection();

      /* Ask server to abort the connection */
      try {
        connection.createStatement().execute("KILL CONNECTION_ID()");
      } catch (Exception e) {
        /* exception is expected here, server sends query aborted */
      }

      /* Try to read  after server side closed the connection */
      connection.createStatement().execute("SELECT 1");

      fail("should never get there");
    } finally {
      if (pc != null) {
        pc.close();
      }
    }
  }

  @Test
  public void testPooledConnectionStatementError() throws Exception {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    ConnectionPoolDataSource ds =
        new MariaDbDataSource(hostname != null ? hostname : "localhost", port, database);
    PooledConnection pc = ds.getPooledConnection(username, password);
    MyEventListener listener = new MyEventListener();
    pc.addStatementEventListener(listener);
    MariaDbConnection connection = (MariaDbConnection) pc.getConnection();
    try (PreparedStatement ps = connection.prepareStatement("SELECT ?")) {
      ps.execute();
      fail("should never get there");
    } catch (Exception e) {
      assertTrue(listener.statementErrorOccured);
      if (sharedBulkCapacity()) {
        assertTrue(
            e.getMessage().contains("Parameter at position 1 is not set")
                || e.getMessage().contains("Incorrect arguments to mysqld_stmt_execute"));
      } else {
        // HY000 if server >= 10.2 ( send prepare and query in a row), 07004 otherwise
        assertTrue(
            "07004".equals(listener.sqlException.getSQLState())
                || "HY000".equals(listener.sqlException.getSQLState()));
      }
    }
    assertTrue(listener.statementClosed);
    pc.close();
  }
}
