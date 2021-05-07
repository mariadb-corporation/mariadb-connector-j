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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;

public class MonoServerFailoverTest extends BaseMonoServer {

  /** Initialisation. */
  @BeforeClass()
  public static void beforeClass2() {
    Assume.assumeTrue(initialUrl != null);
  }

  /** Initialisation. */
  @Before
  public void init() {
    Assume.assumeTrue(
        initialUrl != null
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    defaultUrl = initialUrl;
    currentType = HaMode.NONE;
  }

  @Test
  public void checkClosedConnectionAfterFailover() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {

      Statement st = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);
      try {
        st.execute("SELECT 1");
        fail();
      } catch (SQLException e) {
        // normal exception
      }
      assertTrue(st.isClosed());
      restartProxy(masterServerId);
      try {
        st = connection.createStatement();
        st.execute("SELECT 1");
      } catch (SQLException e) {
        fail();
      }
    }
  }

  @Test
  public void checkErrorAfterDeconnection() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {

      Statement st = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);
      try {
        st.execute("SELECT 1");
        fail();
      } catch (SQLException e) {
        // normal exception
      }

      restartProxy(masterServerId);
      try {
        st.execute("SELECT 1");
        fail();
      } catch (SQLException e) {
        // statement must be closed -> error
      }
      assertTrue(connection.isClosed());
    }
  }

  @Test
  public void checkAutoReconnectDeconnection() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {

      Statement st = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);
      try {
        st.execute("SELECT 1");
        fail();
      } catch (SQLException e) {
        // normal exception
      }

      restartProxy(masterServerId);
      try {
        // with autoreconnect -> not closed
        st = connection.createStatement();
        st.execute("SELECT 1");
      } catch (SQLException e) {
        fail();
      }
      assertFalse(connection.isClosed());
    }
  }

  /**
   * CONJ-120 Fix Connection.isValid method
   *
   * @throws Exception exception
   */
  @Test
  public void isValidConnectionThatIsKilledExternally() throws Throwable {
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !"maxscale".equals(System.getenv("srv")));
    try (Connection connection = getNewConnection()) {
      connection.setCatalog("mysql");
      Protocol protocol = getProtocolFromConnection(connection);
      try (Connection killerConnection = getNewConnection()) {
        Statement killerStatement = killerConnection.createStatement();
        long threadId = protocol.getServerThreadId();
        killerStatement.execute("KILL CONNECTION " + threadId);
        boolean isValid = connection.isValid(0);
        assertFalse(isValid);
      }
    }
  }

  @Test
  public void checkPrepareStatement() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists failt1");
      stmt.execute("create table failt1 (id int not null primary key auto_increment, tt int)");
      stmt.execute("FLUSH TABLES");
      PreparedStatement preparedStatement =
          connection.prepareStatement("insert into failt1(id, tt) values (?,?)");

      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);

      preparedStatement.setInt(1, 1);
      preparedStatement.setInt(2, 1);
      preparedStatement.addBatch();
      try {
        preparedStatement.executeBatch();
        fail();
      } catch (SQLException e) {
        // normal exception
      }
      restartProxy(masterServerId);
      stmt.execute("SELECT 1");
    }
  }
}
