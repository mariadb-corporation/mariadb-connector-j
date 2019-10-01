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

package org.mariadb.jdbc.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.util.constant.HaMode;

public class ReplicationFailoverTest extends BaseReplication {

  /** Initialisation. */
  @BeforeClass()
  public static void beforeClass2() {
    proxyUrl = proxyReplicationUrl;
    Assume.assumeTrue(initialReplicationUrl != null);
  }

  /** Initialisation. */
  @Before
  public void init() {
    defaultUrl = initialReplicationUrl;
    currentType = HaMode.REPLICATION;
  }

  @Test
  public void readOnlyPropagatesToServerAlias() throws SQLException {
    assureReadOnly(true);
  }

  @Test
  public void assureReadOnly() throws SQLException {
    assureReadOnly(false);
  }

  /**
   * Test assureReadOnly / readOnlyPropagatesToServer alias.
   *
   * @param useAlias use alias readOnlyPropagatesToServer ?
   * @throws SQLException if any exception
   */
  public void assureReadOnly(boolean useAlias) throws SQLException {
    try (Connection connection =
        getNewConnection(
            useAlias ? "&readOnlyPropagatesToServer=true" : "&assureReadOnly=true", false)) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists replicationDelete" + jobId);
      stmt.execute(
          "create table replicationDelete"
              + jobId
              + " (id int not null primary key auto_increment, test VARCHAR(10))");
      connection.setReadOnly(true);
      assertTrue(connection.isReadOnly());
      try {
        if (!isMariaDbServer(connection) || !requireMinimumVersion(connection, 5, 7)) {
          // on version >= 5.7 use SESSION READ-ONLY, before no control
          Assume.assumeTrue(false);
        }
        connection.createStatement().execute("drop table  if exists replicationDelete" + jobId);
        fail();
      } catch (SQLException e) {
        // normal exception
      }
    }
  }

  @Test
  public void pingReconnectAfterFailover() throws Throwable {
    try (Connection connection =
        getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
      Statement st = connection.createStatement();
      final int masterServerId = getServerId(connection);
      stopProxy(masterServerId);

      try {
        st.execute("SELECT 1");
      } catch (SQLException e) {
        // normal exception
      }

      connection.setReadOnly(true);
      restartProxy(masterServerId);
      try {
        connection.setReadOnly(false);
      } catch (SQLException e) {
        fail();
      }
    }
  }

  @Test
  public void failoverDuringMasterSetReadOnly() throws Throwable {
    try (Connection connection =
        getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);
      connection.setReadOnly(true);
      int slaveServerId = getServerId(connection);
      assertFalse(slaveServerId == masterServerId);
      assertTrue(connection.isReadOnly());
    }
  }

  @Test(expected = SQLException.class)
  public void masterWithoutFailover() throws Throwable {
    try (Connection connection =
        getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
      int masterServerId = getServerId(connection);
      connection.setReadOnly(true);
      int firstSlaveId = getServerId(connection);
      connection.setReadOnly(false);

      stopProxy(masterServerId);
      stopProxy(firstSlaveId);

      connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
      fail();
    }
  }

  @Test
  public void checkBackOnMasterOnSlaveFail() throws Throwable {
    try (Connection connection =
        getNewConnection(
            "&retriesAllDown=6&failOnReadOnly=true&connectTimeout=1000&socketTimeout=1000", true)) {
      Statement st = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);

      try {
        st.execute("SELECT 1");
        assertTrue(connection.isReadOnly());
      } catch (SQLException e) {
        fail();
      }

      long stoppedTime = System.nanoTime();
      restartProxy(masterServerId);
      boolean loop = true;
      while (loop) {
        Thread.sleep(250);
        try {
          connection.setReadOnly(true);
          loop = false;
        } catch (SQLException e) {
          // eat exception
        }
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedTime);
        if (duration > 15 * 1000) {
          fail();
        }
      }
    }
  }

  @Test
  public void testFailNotOnSlave() throws Throwable {
    try (Connection connection =
        getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
      Statement stmt = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);
      try {
        stmt.execute("SELECT 1");
        fail();
      } catch (SQLException e) {
        // normal error
      }
      assertFalse(connection.isReadOnly());
    }
  }

  @Test
  public void commitExecutionOnSlave() throws SQLException {
    try (Connection conn = getNewConnection()) {
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE IF NOT EXISTS commitExecution(id int, val varchar(256))");
      stmt.execute("TRUNCATE TABLE commitExecution");
      stmt.execute("INSERT INTO commitExecution value (1, 'test')");
      conn.setAutoCommit(false);
      assertFalse(conn.getAutoCommit());
      conn.setReadOnly(true);
      assertFalse(conn.getAutoCommit());

      ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM commitExecution");
      rs.next();
      assertEquals(rs.getInt(1), 1);

      conn.setReadOnly(false);
      conn.createStatement().execute("INSERT INTO commitExecution value (2, 'test3')");
      conn.commit();
      conn.setReadOnly(true);
      conn.commit();
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM commitExecution");
      rs.next();
      assertEquals(rs.getInt(1), 2);
    }
  }
}
