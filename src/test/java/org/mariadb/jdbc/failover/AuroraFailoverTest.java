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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;

/**
 * Aurora test suite. Some environment parameter must be set : - defaultAuroraUrl : example
 * -DdefaultAuroraUrl=jdbc:mariadb:aurora://instance-1.xxxx,instance-2.xxxx/testj?user=userName&password=userPwd
 * - AURORA_ACCESS_KEY = access key - AURORA_SECRET_KEY = secret key - AURORA_CLUSTER_IDENTIFIER =
 * cluster identifier. example : -DAURORA_CLUSTER_IDENTIFIER=instance-1-cluster
 *
 * <p>"AURORA" environment variable must be set to a value
 */
public class AuroraFailoverTest extends BaseReplication {

  /** Initialisation. */
  @BeforeClass()
  public static void beforeClass2() {
    proxyUrl = proxyAuroraUrl;
    Assume.assumeTrue(initialAuroraUrl != null);
  }

  /** Initialisation. */
  @Before
  public void init() {
    defaultUrl = initialAuroraUrl;
    currentType = HaMode.AURORA;
  }

  @Test
  public void testErrorWriteOnReplica() throws SQLException {
    try (Connection connection = getNewConnection(false)) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists auroraDelete" + jobId);
      stmt.execute(
          "create table auroraDelete"
              + jobId
              + " (id int not null primary key auto_increment, test VARCHAR(10))");
      connection.setReadOnly(true);
      assertTrue(connection.isReadOnly());
      try {
        stmt.execute("drop table if exists auroraDelete" + jobId);
        System.out.println(
            "ERROR - > must not be able to write on slave. check if you database is start with --read-only");
        fail();
      } catch (SQLException e) {
        // normal exception
        connection.setReadOnly(false);
        stmt.execute("drop table if exists auroraDelete" + jobId);
      }
    }
  }

  @Test
  public void testReplication() throws SQLException, InterruptedException {
    try (Connection connection = getNewConnection(false)) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists auroraReadSlave" + jobId);
      stmt.execute(
          "create table auroraReadSlave"
              + jobId
              + " (id int not null primary key auto_increment, test VARCHAR(10))");

      // wait to be sure slave have replicate data
      Thread.sleep(1500);

      connection.setReadOnly(true);
      ResultSet rs = stmt.executeQuery("Select count(*) from auroraReadSlave" + jobId);
      assertTrue(rs.next());
      connection.setReadOnly(false);
      stmt.execute("drop table  if exists auroraReadSlave" + jobId);
    }
  }

  @Test
  public void testReadOnly() throws SQLException {
    try (Connection connection = getNewConnection(false)) {
      ResultSet rs = connection.createStatement().executeQuery("select @@innodb_read_only");

      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
      assertFalse(connection.isReadOnly());

      connection.setReadOnly(true);

      rs = connection.createStatement().executeQuery("select @@innodb_read_only");

      assertTrue(rs.next());
      assertNotEquals(0, rs.getInt(1));
      assertTrue(connection.isReadOnly());
    }
  }

  @Test
  public void testFailMaster() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=3&connectTimeout=1000", true)) {
      int previousPort = getProtocolFromConnection(connection).getPort();
      Statement stmt = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);
      long stopTime = System.nanoTime();
      try {
        // Handles failover so may connect to another and is still able to execute
        stmt.execute("SELECT 1");
        if (getProtocolFromConnection(connection).getPort() == previousPort) {
          fail();
        }
      } catch (SQLException e) {
        // normal error
      }
      assertFalse(connection.isReadOnly());
      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopTime);
      assertTrue(duration < 25 * 1000);
    }
  }

  /**
   * Conj-79.
   *
   * @throws SQLException exception
   */
  @Test
  public void socketTimeoutTest() throws SQLException {
    // set a short connection timeout
    try (Connection connection = getNewConnection("&socketTimeout=4000", false)) {

      PreparedStatement ps = connection.prepareStatement("SELECT 1");
      ResultSet rs = ps.executeQuery();
      assertTrue(rs.next());

      // wait for the connection to time out
      ps = connection.prepareStatement("DO sleep(20)");

      // a timeout should occur here
      try {
        ps.executeQuery();
        fail();
      } catch (SQLException e) {
        // check that it's a timeout that occurs
        assertTrue(e.getMessage().contains("timed out"));
      }
      try {
        ps = connection.prepareStatement("SELECT 2");
        ps.execute();
      } catch (Exception e) {
        fail();
      }

      try {
        ps.executeQuery();
      } catch (SQLException e) {
        fail();
      }

      // the connection should not be closed
      assertFalse(connection.isClosed());
    }
  }

  /** Conj-166 Connection error code must be thrown. */
  @Test
  public void testAccessDeniedErrorCode() {
    try {
      DriverManager.getConnection(defaultUrl + "&retriesAllDown=6", "foouser", "foopwd");
      fail();
    } catch (SQLException e) {
      System.out.println(e.getSQLState());
      System.out.println(e.getErrorCode());
      assertTrue("28000".equals(e.getSQLState()));
      assertEquals(1045, e.getErrorCode());
    }
  }

  @Test
  public void testClearBlacklist() throws Throwable {
    try (Connection connection = getNewConnection(true)) {
      connection.setReadOnly(true);
      int current = getServerId(connection);
      stopProxy(current);
      Statement st = connection.createStatement();
      try {
        st.execute("SELECT 1 ");
        // switch connection to master -> slave blacklisted
      } catch (SQLException e) {
        fail("must not have been here");
      }

      Protocol protocol = getProtocolFromConnection(connection);
      assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 1);
      assureBlackList();
      assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
    }
  }

  @Test
  public void testCloseFail() throws Throwable {
    assureBlackList();
    Protocol protocol;
    try (Connection connection = getNewConnection(true)) {
      connection.setReadOnly(true);
      int current = getServerId(connection);
      protocol = getProtocolFromConnection(connection);
      assertTrue(
          "Blacklist would normally be zero, but was "
              + protocol.getProxy().getListener().getBlacklistKeys().size(),
          protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
      stopProxy(current);
    }
    // check that after error connection have not been put to blacklist
    assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
  }
}
