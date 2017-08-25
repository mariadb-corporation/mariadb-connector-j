/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.MariaDbPreparedStatementServer;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Aurora test suite.
 * Some environment parameter must be set :
 * - defaultAuroraUrl : example -DdefaultAuroraUrl=jdbc:mariadb:aurora://instance-1.xxxx,instance-2.xxxx/testj?user=userName&password=userPwd
 * - AURORA_ACCESS_KEY = access key
 * - AURORA_SECRET_KEY = secret key
 * - AURORA_CLUSTER_IDENTIFIER = cluster identifier. example : -DAURORA_CLUSTER_IDENTIFIER=instance-1-cluster
 * <p>
 * "AURORA" environment variable must be set to a value
 */
public class AuroraFailoverTest extends BaseReplication {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxyAuroraUrl;
        System.out.println("environment variable \"AURORA\" value : " + System.getenv("AURORA"));
        Assume.assumeTrue(initialAuroraUrl != null && System.getenv("AURORA") != null && amazonRDSClient != null);
    }

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
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
            stmt.execute("create table auroraDelete" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");
            connection.setReadOnly(true);
            assertTrue(connection.isReadOnly());
            try {
                stmt.execute("drop table if exists auroraDelete" + jobId);
                System.out.println("ERROR - > must not be able to write on slave. check if you database is start with --read-only");
                fail();
            } catch (SQLException e) {
                //normal exception
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
            stmt.execute("create table auroraReadSlave" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");

            //wait to be sure slave have replicate data
            Thread.sleep(1500);

            connection.setReadOnly(true);
            ResultSet rs = stmt.executeQuery("Select count(*) from auroraReadSlave" + jobId);
            assertTrue(rs.next());
            connection.setReadOnly(false);
            stmt.execute("drop table  if exists auroraReadSlave" + jobId);
        }
    }


    @Test
    public void testReadOnly() throws SQLException, InterruptedException {
        try (Connection connection = getNewConnection(false)) {
            ResultSet rs = connection.createStatement().executeQuery("show global variables like 'innodb_read_only'");

            assertTrue(rs.next());
            assertEquals("OFF", rs.getString(2));
            assertFalse(connection.isReadOnly());

            connection.setReadOnly(true);

            rs = connection.createStatement().executeQuery("show global variables like 'innodb_read_only'");

            assertTrue(rs.next());
            assertNotEquals("OFF", rs.getString(2));
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
                //normal error
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

    /**
     * Conj-166
     * Connection error code must be thrown.
     *
     * @throws SQLException exception
     */
    @Test
    public void testAccessDeniedErrorCode() throws SQLException {
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
                //switch connection to master -> slave blacklisted
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
            assertTrue("Blacklist would normally be zero, but was " + protocol.getProxy().getListener().getBlacklistKeys().size(),
                    protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
            stopProxy(current);
        }
        //check that after error connection have not been put to blacklist
        assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
    }

    /**
     * Test failover on prepareStatement on slave.
     * PrepareStatement must fall back on master, and back on slave when a new slave connection is up again.
     *
     * @throws Throwable if any error occur
     */
    @Test
    public void failoverPrepareStatementOnSlave() throws Throwable {
        try (Connection connection = getNewConnection("&validConnectionTimeout=120"
                + "&socketTimeout=1000"
                + "&failoverLoopRetries=120"
                + "&connectTimeout=250"
                + "&loadBalanceBlacklistTimeout=50", false)) {

            connection.setReadOnly(true);

            //prepareStatement on slave connection
            PreparedStatement preparedStatement = connection.prepareStatement("select @@innodb_read_only as is_read_only, CONNECTION_ID() as connId");
            ResultSet rs1 = preparedStatement.executeQuery();
            rs1.next();
            int currentConnectionId = rs1.getInt(2);
            boolean isMaster;

            int lastConnectionId = currentConnectionId;

            launchAuroraFailover();

            //test failover
            int nbExecutionOnSlave = 0;
            int nbExecutionOnMasterFirstFailover = 0;

            //Goal is to check that on a failover, master connection will be used, and slave will be used back when up.
            //check on 2 failover
            while (nbExecutionOnSlave + nbExecutionOnMasterFirstFailover < 500) {
                ResultSet rs = preparedStatement.executeQuery();
                assertTrue(rs.next());
                isMaster = rs.getInt(1) != 1;
                currentConnectionId = rs.getInt(2);

                if (lastConnectionId != currentConnectionId) {
                    lastConnectionId = currentConnectionId;
                    if (isMaster) {
                        //temporary use master, wait for au back on slave when reconnected
                        nbExecutionOnMasterFirstFailover++;
                    } else {
                        //master wasn't available too, so reconnected another slave (rare)
                        nbExecutionOnSlave++;
                        break;
                    }
                } else {
                    if (isMaster) {
                        nbExecutionOnMasterFirstFailover++;
                    } else {
                        nbExecutionOnSlave++;
                        if (nbExecutionOnMasterFirstFailover > 0) break;
                    }
                }
            }

            assertTrue("prepare never get back on slave", nbExecutionOnSlave + nbExecutionOnMasterFirstFailover < 500);

            launchAuroraFailover();
            nbExecutionOnSlave = 0;

            int nbExecutionOnMasterSecondFailover = 0;

            while (nbExecutionOnSlave + nbExecutionOnMasterSecondFailover < 500) {
                ResultSet rs = preparedStatement.executeQuery();
                assertTrue(rs.next());
                isMaster = rs.getInt(1) != 1;
                currentConnectionId = rs.getInt(2);

                if (lastConnectionId != currentConnectionId) {
                    if (isMaster) {
                        //temporary use master, wait for au back on slave when reconnected
                        nbExecutionOnMasterSecondFailover++;
                    } else {
                        //master wasn't available too, so reconnected another slave (rare)
                        nbExecutionOnSlave++;
                        break;
                    }
                } else {
                    if (isMaster) {
                        nbExecutionOnMasterSecondFailover++;
                    } else {
                        nbExecutionOnSlave++;
                        if (nbExecutionOnMasterSecondFailover > 0) break;
                    }
                }
            }


            assertTrue("prepare never get back on slave", nbExecutionOnSlave + nbExecutionOnMasterSecondFailover < 500);

            Thread.sleep(2000); //sleep because failover may not be completely finished
        }
    }


    /**
     * Test that master complete failover (not just a network error) server will changed, PrepareStatement will be closed
     * and that PrepareStatement cache is invalidated.
     *
     * @throws Throwable if any error occur
     */
    @Test
    public void failoverPrepareStatementOnMasterWithException() throws Throwable {
        try (Connection connection = getNewConnection("&validConnectionTimeout=120"
                + "&socketTimeout=1000"
                + "&failoverLoopRetries=120"
                + "&connectTimeout=250"
                + "&loadBalanceBlacklistTimeout=50"
                + "&useBatchMultiSend=false", false)) {

            int nbExceptionBeforeUp = 0;
            boolean failLaunched = false;
            PreparedStatement preparedStatement1 = connection.prepareStatement("select ?");
            assertEquals(1L, getPrepareResult((MariaDbPreparedStatementServer) preparedStatement1).getStatementId());
            connection.prepareStatement(" select 1");

            while (nbExceptionBeforeUp < 1000) {
                try {
                    PreparedStatement preparedStatement = connection.prepareStatement(" select 1");
                    preparedStatement.executeQuery();
                    long currentPrepareId = getPrepareResult((MariaDbPreparedStatementServer) preparedStatement).getStatementId();
                    if (nbExceptionBeforeUp > 0) {
                        assertEquals(1L, currentPrepareId);
                        break;
                    }
                    if (!failLaunched) {
                        launchAuroraFailover();
                        failLaunched = true;
                    }
                    assertEquals(2, currentPrepareId);

                } catch (SQLException e) {
                    nbExceptionBeforeUp++;
                }
            }
            assertTrue(nbExceptionBeforeUp < 50);
        }
    }

    /**
     * Same than failoverPrepareStatementOnMasterWithException, but since query is a select, mustn't throw an exception.
     *
     * @throws Throwable if any error occur
     */
    @Test
    public void failoverPrepareStatementOnMaster() throws Throwable {
        try (Connection connection = getNewConnection("&validConnectionTimeout=120"
                + "&socketTimeout=1000"
                + "&failoverLoopRetries=120"
                + "&connectTimeout=250"
                + "&loadBalanceBlacklistTimeout=50"
                + "&useBatchMultiSend=false", false)) {

            int nbExecutionBeforeRePrepared = 0;
            boolean failLaunched = false;
            PreparedStatement preparedStatement1 = connection.prepareStatement("select ?");
            assertEquals(1L, getPrepareResult((MariaDbPreparedStatementServer) preparedStatement1).getStatementId());
            connection.prepareStatement("select @@innodb_read_only as is_read_only");
            long currentPrepareId = 0;
            while (nbExecutionBeforeRePrepared < 1000) {
                PreparedStatement preparedStatement = connection.prepareStatement("select @@innodb_read_only as is_read_only");
                preparedStatement.executeQuery();
                currentPrepareId = getPrepareResult((MariaDbPreparedStatementServer) preparedStatement).getStatementId();

                if (nbExecutionBeforeRePrepared == 0) {
                    assertEquals(2, currentPrepareId);
                } else {
                    if (!failLaunched) {
                        launchAuroraFailover();
                        failLaunched = true;
                    }
                    if (currentPrepareId == 1) break;
                }
                nbExecutionBeforeRePrepared++;
            }
            assertEquals(1, currentPrepareId);
            assertTrue(nbExecutionBeforeRePrepared < 200);
        }
    }
}
