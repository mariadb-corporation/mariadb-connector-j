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
import org.junit.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbPreparedStatementServer;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public abstract class BaseReplication extends BaseMonoServer {

    @Test
    public void failoverSlaveToMasterPrepareStatement() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection(
                    "&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000&useBatchMultiSend=false", true);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists replicationFailoverBinary" + jobId);
            stmt.execute("create table replicationFailoverBinary" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");
            stmt.execute("insert into replicationFailoverBinary" + jobId + "(test) values ('Harriba !')");
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            //wait for table replication on slave
            Thread.sleep(200);

            //create another prepareStatement, to permit to verify that prepare id has changed
            connection.prepareStatement("SELECT ?");

            //prepareStatement on slave connection
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT test from replicationFailoverBinary" + jobId + " where id = ?");
            final long currentPrepareId = getPrepareResult((MariaDbPreparedStatementServer) preparedStatement).getStatementId();
            int slaveServerId = getServerId(connection);
            assertFalse(masterServerId == slaveServerId);
            //stop slave for a few seconds
            stopProxy(slaveServerId, 2000);

            //test failover
            preparedStatement.setInt(1, 1);
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            assertEquals("Harriba !", rs.getString(1));
            assertNotEquals(currentPrepareId, getPrepareResult((MariaDbPreparedStatementServer) preparedStatement).getStatementId());

            int currentServerId = getServerId(connection);

            assertTrue(masterServerId == currentServerId);
            assertFalse(connection.isReadOnly());
            Thread.sleep(2000);
            boolean hasReturnOnSlave = false;

            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                preparedStatement.setInt(1, 1);
                rs = preparedStatement.executeQuery();
                rs.next();
                assertEquals("Harriba !", rs.getString(1));

                currentServerId = getServerId(connection);
                if (currentServerId != masterServerId) {
                    hasReturnOnSlave = true;
                    assertTrue(connection.isReadOnly());
                    break;
                }
            }
            assertTrue("Prepare statement has not return on Slave", hasReturnOnSlave);
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test()
    public void failoverSlaveAndMasterRewrite() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection(
                    "&rewriteBatchedStatements=true&retriesAllDown=6&connectTimeout=2000&socketTimeout=2000", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            //stop proxy for 2s
            stopProxy(firstSlaveId, 4000);

            try {
                Statement stmt = connection.createStatement();
                stmt.addBatch("DO 1");
                stmt.addBatch("DO 2");
                int[] resultData = stmt.executeBatch();
                int secondSlaveId = getServerId(connection);
                assertEquals("the 2 batch queries must have been executed when failover", 2, resultData.length);
                assertTrue(secondSlaveId != firstSlaveId && secondSlaveId != masterServerId);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverSlaveToMaster() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            assertFalse(masterServerId == slaveServerId);
            stopProxy(slaveServerId);
            connection.createStatement().execute("SELECT 1");
            int currentServerId = getServerId(connection);

            assertTrue(masterServerId == currentServerId);
            assertFalse(connection.isReadOnly());
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverDuringSlaveSetReadOnly() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&socketTimeout=3000", true);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);

            stopProxy(slaveServerId, 2000);
            connection.setReadOnly(false);
            int masterServerId = getServerId(connection);

            assertFalse(slaveServerId == masterServerId);
            assertFalse(connection.isReadOnly());
        } finally {
            if (connection != null) connection.close();
        }
        Thread.sleep(2500); //for not interfering with other tests
    }

    @Test()
    public void failoverSlaveAndMasterWithoutAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=20&connectTimeout=2000&socketTimeout=2000", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            try {
                //will connect to second slave that isn't stopped
                connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void reconnectSlaveAndMasterWithAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection(
                    "&retriesAllDown=6&connectTimeout=2000&socketTimeout=2000", true);
            //search actual server_id for master and slave
            int masterServerId = getServerId(connection);

            connection.setReadOnly(true);

            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            //must reconnect to the second slave without error
            connection.createStatement().execute("SELECT 1");
            int currentSlaveId = getServerId(connection);
            assertTrue(currentSlaveId != firstSlaveId);
            assertTrue(currentSlaveId != masterServerId);
        } finally {
            if (connection != null) connection.close();
        }
    }


    @Test
    public void failoverMasterWithAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection(
                    "&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            int masterServerId = getServerId(connection);

            stopProxy(masterServerId, 250);
            //with autoreconnect, the connection must reconnect automatically
            int currentServerId = getServerId(connection);

            assertTrue(currentServerId == masterServerId);
            assertFalse(connection.isReadOnly());
        } finally {
            if (connection != null) connection.close();
        }
        Thread.sleep(500); //for not interfering with other tests
    }

    @Test
    public void writeToSlaveAfterFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            //if super user can write on slave
            Assume.assumeTrue(!hasSuperPrivilege(connection, "writeToSlaveAfterFailover"));
            Statement st = connection.createStatement();
            st.execute("drop table  if exists writeToSlave" + jobId);
            st.execute("create table writeToSlave" + jobId + " (id int not null primary key , amount int not null) ENGINE = InnoDB");
            st.execute("insert into writeToSlave" + jobId + " (id, amount) VALUE (1 , 100)");

            int masterServerId = getServerId(connection);

            stopProxy(masterServerId);
            try {
                st.execute("insert into writeToSlave" + jobId + " (id, amount) VALUE (2 , 100)");
                fail();
            } catch (SQLException e) {
                //normal exception
                restartProxy(masterServerId);
                st = connection.createStatement();
                st.execute("drop table if exists writeToSlave" + jobId);
            }
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void randomConnection() throws Throwable {
        Map<HostAddress, MutableInt> connectionMap = new HashMap<HostAddress, MutableInt>();
        int masterId = -1;
        for (int i = 0; i < 20; i++) {
            Connection connection = null;
            try {
                connection = getNewConnection(false);
                int serverId = getServerId(connection);
                if (i > 0) {
                    assertTrue(masterId == serverId);
                }
                masterId = serverId;
                connection.setReadOnly(true);
                HostAddress replicaHost = getServerHostAddress(connection);
                MutableInt count = connectionMap.get(replicaHost);
                if (count == null) {
                    connectionMap.put(replicaHost, new MutableInt());
                } else {
                    count.increment();
                }
            } finally {
                if (connection != null) connection.close();
            }
        }

        assertTrue(connectionMap.size() >= 2);
        for (HostAddress key : connectionMap.keySet()) {
            Integer connectionCount = connectionMap.get(key).get();
            assertTrue(connectionCount > 1);
        }

    }

    @Test
    public void closeWhenInReconnectionLoop() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000", true);
            int masterId = getServerId(connection);
            connection.setReadOnly(true);
            //close all slave proxy
            stopProxyButParameter(masterId);

            //trigger the failover, so a failover thread is launched
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT 1");

            //launch connection close during failover must not throw error
            Thread.sleep(200);
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverSlaveToMasterFail() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            assertTrue(slaveServerId != masterServerId);

            connection.setCatalog("mysql"); //to be sure there will be a query, and so an error when switching connection
            stopProxy(masterServerId);
            try {
                //must throw error
                connection.setReadOnly(false);
                fail();
            } catch (SQLException e) {
                //normal exception
            }
            restartProxy(masterServerId);
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6", true);
            int masterServerId = -1;
            masterServerId = getServerId(connection);

            stopProxy(masterServerId);

            connection.setReadOnly(true);

            int slaveServerId = getServerId(connection);

            assertFalse(slaveServerId == masterServerId);
            assertTrue(connection.isReadOnly());
            restartProxy(masterServerId);
        } finally {
            if (connection != null) connection.close();
        }
    }

    class MutableInt {

        private int value = 1; // note that we start at 1 since we're counting

        public void increment() {
            ++value;
        }

        public int get() {
            return value;
        }
    }
}
