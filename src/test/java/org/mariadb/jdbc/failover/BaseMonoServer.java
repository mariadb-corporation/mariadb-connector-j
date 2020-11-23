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

package org.mariadb.jdbc.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public abstract class BaseMonoServer extends BaseMultiHostTest {

  @Test
  public void testWriteOnMaster() throws SQLException {
    try (Connection connection = getNewConnection(false)) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists auroraMultiNode" + jobId);
      stmt.execute(
          "create table auroraMultiNode"
              + jobId
              + " (id int not null primary key auto_increment, test VARCHAR(10))");
      stmt.execute("drop table  if exists auroraMultiNode" + jobId);
    }
  }

  @Test
  public void relaunchWithoutError() throws Throwable {
    try (Connection connection =
        getNewConnection("&connectTimeout=1000&socketTimeout=1000", true)) {
      Statement st = connection.createStatement();
      int masterServerId = getServerId(connection);
      long startTime = System.currentTimeMillis();
      stopProxy(masterServerId, 4000);
      try {
        st.execute("SELECT 1");
        if (System.currentTimeMillis() - startTime < 4 * 1000) {
          fail(
              "Auto-reconnection must have been done after 4000ms but was "
                  + (System.currentTimeMillis() - startTime));
        }
      } catch (SQLException e) {
        fail("must not have thrown error");
      }
    }
  }

  @Test
  public void relaunchWithErrorWhenInTransaction() throws Throwable {
    try (Connection connection =
        getNewConnection("&connectTimeout=1000&socketTimeout=1000", true)) {
      Statement st = connection.createStatement();
      st.execute("drop table if exists baseReplicationTransaction" + jobId);
      st.execute(
          "create table baseReplicationTransaction"
              + jobId
              + " (id int not null primary key auto_increment, test VARCHAR(10))");
      st.execute("FLUSH TABLES");
      connection.setAutoCommit(false);
      st.execute("INSERT INTO baseReplicationTransaction" + jobId + "(test) VALUES ('test')");
      int masterServerId = getServerId(connection);
      st.execute("SELECT 1");
      long startTime = System.currentTimeMillis();
      stopProxy(masterServerId, 2000);
      try {
        st.execute("SELECT 1");
        fail("must have thrown error since in transaction that is lost");
      } catch (SQLException e) {
        assertEquals(
            "error type not normal after " + (System.currentTimeMillis() - startTime) + "ms",
            "25S03",
            e.getSQLState());
      }
      Thread.sleep(2500);
      st.execute("drop table if exists baseReplicationTransaction" + jobId);
    }
  }

  @Test
  public void failoverRelaunchedWhenSelect() throws Throwable {
    try (Connection connection =
        getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true)) {
      Statement st = connection.createStatement();

      final int masterServerId = getServerId(connection);
      st.execute("drop table if exists selectFailover" + jobId);
      st.execute(
          "create table selectFailover"
              + jobId
              + " (id int not null primary key , amount int not null) "
              + "ENGINE = InnoDB");
      st.execute("FLUSH TABLES");
      stopProxy(masterServerId, 2);
      try {
        st.execute("SELECT * from selectFailover" + jobId);
      } catch (SQLException e) {
        e.printStackTrace();
        fail("must not have thrown error");
      }

      stopProxy(masterServerId, 2);
      try {
        st.execute("INSERT INTO selectFailover" + jobId + " VALUES (1,2)");
        fail("not have thrown error !");
      } catch (SQLException e) {
        restartProxy(masterServerId);
        assertEquals("error type not normal", "25S03", e.getSQLState());
      }
    }
  }

  @Test
  public void failoverRelaunchedWhenInTransaction() throws Throwable {
    try (Connection connection =
        getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true)) {
      Statement st = connection.createStatement();

      final int masterServerId = getServerId(connection);
      st.execute("drop table if exists selectFailoverTrans" + jobId);
      st.execute(
          "create table selectFailoverTrans"
              + jobId
              + " (id int not null primary key , amount int not null) "
              + "ENGINE = InnoDB");
      st.execute("FLUSH TABLES");
      connection.setAutoCommit(false);
      st.execute("INSERT INTO selectFailoverTrans" + jobId + " VALUES (0,0)");
      stopProxy(masterServerId, 2);
      try {
        st.execute("SELECT * from selectFailoverTrans" + jobId);
        fail("not have thrown error !");
      } catch (SQLException e) {
        assertEquals("error type not normal", "25S03", e.getSQLState());
      }

      stopProxy(masterServerId, 2);
      try {
        st.execute("INSERT INTO selectFailoverTrans" + jobId + " VALUES (1,2)");
        fail("not have thrown error !");
      } catch (SQLException e) {
        restartProxy(masterServerId);
        st.execute("drop table if exists selectFailoverTrans" + jobId);
        assertEquals("error type not normal", "25S03", e.getSQLState());
      }
    }
  }

  @Test
  public void pingReconnectAfterRestart() throws Throwable {
    try (Connection connection =
        getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true)) {
      Statement st = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);

      try {
        st.execute("SELECT 1");
      } catch (SQLException e) {
        // normal exception
      }
      restartProxy(masterServerId);
      long restartTime = System.nanoTime();

      boolean loop = true;
      while (loop) {
        if (!connection.isClosed()) {
          loop = false;
        }
        connection.createStatement();
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - restartTime);
        if (duration > 20 * 1000) {
          fail("Auto-reconnection not done after " + duration);
        }
        Thread.sleep(250);
      }
    }
  }
}
