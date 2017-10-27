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
 */

package org.mariadb.jdbc;

import org.junit.Assume;
import org.junit.Test;
import org.mariadb.jdbc.internal.util.pool.Pools;
import org.mariadb.jdbc.internal.util.scheduler.MariaDbThreadFactory;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class MariaDbPoolDataSourceTest extends BaseTest {

    @Test
    public void testResetDatabase() throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1");

        Connection connection = pool.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE DATABASE IF NOT EXISTS testingReset");
        connection.setCatalog("testingReset");
        connection.close();

        connection = pool.getConnection();
        assertEquals(database, connection.getCatalog());
        statement = connection.createStatement();
        statement.execute("DROP DATABASE testingReset");
        connection.close();

        pool.close();
    }

    @Test
    public void testResetSessionVariable() throws SQLException {
        testResetSessionVariable(false);
        if ( (isMariadbServer() && minVersion(10,2)) || (!isMariadbServer() && minVersion(5,7)) ) {
            testResetSessionVariable(true);
        }
    }

    private void testResetSessionVariable(boolean useResetConnection) throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1&useResetConnection=" + useResetConnection);

        long nowMillis;
        int initialWaitTimeout;

        Connection connection = pool.getConnection();
        Statement statement = connection.createStatement();

        nowMillis = getNowTime(statement);
        initialWaitTimeout = getWaitTimeout(statement);

        statement.execute("SET @@timestamp=UNIX_TIMESTAMP('1970-10-01 01:00:00'), @@wait_timeout=2000");
        long newNowMillis = getNowTime(statement);
        int waitTimeout = getWaitTimeout(statement);

        assertTrue(nowMillis - newNowMillis > 23587200000L);
        assertEquals(2000, waitTimeout);
        connection.close();

        connection = pool.getConnection();
        statement = connection.createStatement();

        newNowMillis = getNowTime(statement);
        waitTimeout = getWaitTimeout(statement);

        if (useResetConnection) {
            assertTrue(nowMillis - newNowMillis < 10L);
            assertEquals(initialWaitTimeout, waitTimeout);
        } else {
            assertTrue(nowMillis - newNowMillis > 23587200000L);
            assertEquals(2000, waitTimeout);
        }
        connection.close();
        pool.close();
    }

    private long getNowTime(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT NOW()");
        assertTrue(rs.next());
        return rs.getTimestamp(1).getTime();
    }

    private int getWaitTimeout(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT @@wait_timeout");
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    @Test
    public void testResetUserVariable() throws SQLException {
        testResetUserVariable(false);
        if ( (isMariadbServer() && minVersion(10,2)) || (!isMariadbServer() && minVersion(5,7)) ) {
            testResetUserVariable(true);
        }
    }

    private void testResetUserVariable(boolean useResetConnection) throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1&useResetConnection=" + useResetConnection);
        long nowMillis;
        Connection connection = pool.getConnection();
        Statement statement = connection.createStatement();
        assertNull(getUserVariableStr(statement));

        statement.execute("SET @str = '123'");

        assertEquals("123", getUserVariableStr(statement));
        connection.close();

        connection = pool.getConnection();
        statement = connection.createStatement();
        if (useResetConnection) {
            assertNull(getUserVariableStr(statement));
        } else {
            assertEquals("123", getUserVariableStr(statement));
        }
        connection.close();
        pool.close();
    }

    private String getUserVariableStr(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT @str");
        assertTrue(rs.next());
        return rs.getString(1);
    }


    @Test
    public void testNetworkTimeout() throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1&socketTimeout=10000");
        MariaDbConnection connection = (MariaDbConnection) pool.getConnection();
        assertEquals(10000, connection.getNetworkTimeout());
        connection.setNetworkTimeout(null, 5000);
        connection.close();


        connection = (MariaDbConnection) pool.getConnection();
        assertEquals(10000, connection.getNetworkTimeout());
        connection.close();
        pool.close();
    }


    @Test
    public void testResetReadOnly() throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1");
        Connection connection = pool.getConnection();
        assertFalse(connection.isReadOnly());
        connection.setReadOnly(true);
        assertTrue(connection.isReadOnly());
        connection.close();

        connection = pool.getConnection();
        assertFalse(connection.isReadOnly());
        connection.close();
        pool.close();
    }

    @Test
    public void testResetAutoCommit() throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1");
        Connection connection = pool.getConnection();
        assertTrue(connection.getAutoCommit());
        connection.setAutoCommit(false);
        assertFalse(connection.getAutoCommit());
        connection.close();

        connection = pool.getConnection();
        assertTrue(connection.getAutoCommit());
        connection.close();
        pool.close();
    }

    @Test
    public void testResetAutoCommitOption() throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1&autocommit=false&poolName=PoolTest");
        Connection connection = pool.getConnection();
        assertFalse(connection.getAutoCommit());
        connection.setAutoCommit(true);
        assertTrue(connection.getAutoCommit());
        connection.close();
        connection = pool.getConnection();
        assertFalse(connection.getAutoCommit());
        pool.close();
    }

    @Test
    public void testResetTransactionIsolation() throws SQLException {
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1");

        Connection connection = pool.getConnection();
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
        connection.close();

        connection = pool.getConnection();
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
        connection.close();
        pool.close();
    }

    @Test
    public void testJmx() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName filter = new ObjectName("org.mariadb.jdbc.pool:type=PoolTestJmx-*");
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=5&minPoolSize=0&poolName=PoolTestJmx");

        final Connection connection = pool.getConnection();
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        checkJmxInfo(server, name, 1, 1, 0, 0);

        Connection connection2 = pool.getConnection();
        checkJmxInfo(server, name, 2, 2, 0, 0);
        connection2.close();

        checkJmxInfo(server, name, 1, 2, 1, 0);
        connection.close();
        pool.close();
    }

    @Test
    public void testNoMinConnection() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName filter = new ObjectName("org.mariadb.jdbc.pool:type=testNoMinConnection-*");
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=5&poolName=testNoMinConnection");
        final Connection connection = pool.getConnection();
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        //wait to ensure pool has time to create 5 connections
        try {
            Thread.sleep(sharedIsAurora() ? 10000 : 500);
        } catch (InterruptedException interruptEx) {
            //eat
        }

        checkJmxInfo(server, name, 1, 5, 4, 0);

        Connection connection2 = pool.getConnection();
        checkJmxInfo(server, name, 2, 5, 3, 0);
        connection2.close();

        checkJmxInfo(server, name, 1, 5, 4, 0);
        connection.close();
        pool.close();
    }

    @Test
    public void testIdleTimeout() throws Throwable {
        Assume.assumeTrue(System.getenv("MAXSCALE_VERSION") == null); //not for maxscale, testing thread id is not relevant.
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName filter = new ObjectName("org.mariadb.jdbc.pool:type=testIdleTimeout-*");
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=5&minPoolSize=3&poolName=testIdleTimeout");

        pool.testForceMaxIdleTime(sharedIsAurora() ? 10 : 3);
        //wait to ensure pool has time to create 3 connections
        Thread.sleep(sharedIsAurora() ? 5000 : 1000);

        Set<ObjectName> objectNames = server.queryNames(filter, null);
        ObjectName name = objectNames.iterator().next();
        checkJmxInfo(server, name, 0, 3, 3, 0);

        List<Long> initialThreadIds = pool.testGetConnectionIdleThreadIds();
        Thread.sleep(sharedIsAurora() ? 12000 : 3500);

        //must still have 3 connections, but must be other ones
        checkJmxInfo(server, name, 0, 3, 3, 0);
        List<Long> threadIds = pool.testGetConnectionIdleThreadIds();
        assertEquals(initialThreadIds.size(), threadIds.size());
        for (Long initialThread : initialThreadIds) {
            assertFalse(threadIds.contains(initialThread));
        }

        pool.close();
    }



    @Test
    public void testMinConnection() throws Throwable {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName filter = new ObjectName("org.mariadb.jdbc.pool:type=testMinConnection-*");
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=5&minPoolSize=3&poolName=testMinConnection");
        final Connection connection = pool.getConnection();
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        //to ensure pool has time to create minimal connection number
        Thread.sleep(sharedIsAurora() ? 5000 : 500);

        checkJmxInfo(server, name, 1, 3, 2, 0);

        Connection connection2 = pool.getConnection();
        checkJmxInfo(server, name, 2, 3, 1, 0);
        connection2.close();

        checkJmxInfo(server, name, 1, 3, 2, 0);
        connection.close();
        pool.close();
    }

    private void checkJmxInfo(MBeanServer server,
                              ObjectName name,
                              long expectedActive,
                              long expectedTotal,
                              long expectedIdle,
                              long expectedRequest)
            throws Exception {

        assertEquals(expectedActive, ((Long) server.getAttribute(name, "ActiveConnections")).longValue());
        assertEquals(expectedTotal, ((Long) server.getAttribute(name, "TotalConnections")).longValue());
        assertEquals(expectedIdle, ((Long) server.getAttribute(name, "IdleConnections")).longValue());
        assertEquals(expectedRequest, ((Long) server.getAttribute(name, "ConnectionRequests")).longValue());
    }

    @Test
    public void testJmxDisable() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName filter = new ObjectName("org.mariadb.jdbc.pool:type=PoolTest-*");
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=2&registerJmxPool=false&poolName=PoolTest");

        Connection connection = pool.getConnection();
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(0, objectNames.size());
        connection.close();

        pool.close();
    }

    @Test
    public void testResetRollback() throws SQLException {
        createTable("testResetRollback", "id int not null primary key auto_increment, test varchar(20)");
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=1");
        Connection connection = pool.getConnection();
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("INSERT INTO testResetRollback (test) VALUES ('heja')");
        connection.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO testResetRollback (test) VALUES ('japp')");
        connection.close();

        connection = pool.getConnection();
        stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM testResetRollback");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        connection.close();

        pool.close();
    }

    @Test
    public void ensureUsingPool() throws Exception {
        ThreadPoolExecutor connectionAppender = new ThreadPoolExecutor(50, 5000, 10, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(5000),
                new MariaDbThreadFactory("testPool"));

        final long start = System.currentTimeMillis();
        final Set<Integer> threadIds = new HashSet<Integer>();
        for (int i = 0; i < 500; i++) {
            connectionAppender.execute(new Runnable() {
                @Override
                public void run() {
                    Connection connection = null;
                    try {
                        connection = DriverManager.getConnection(connUri + "&pool&staticGlobal&poolName=PoolTest");
                        Statement stmt = connection.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT CONNECTION_ID()");
                        rs.next();
                        threadIds.add(rs.getInt(1));
                        stmt.execute("SELECT * FROM mysql.user");

                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
        }
        connectionAppender.shutdown();
        connectionAppender.awaitTermination(sharedIsAurora() ? 200 : 30, TimeUnit.SECONDS);
        assertTrue("connection ids must be less than 8 : " + threadIds.size(), threadIds.size() <= 8);
        assertTrue(System.currentTimeMillis() - start < (sharedIsAurora() ? 120000 : 5000));
        Pools.close("PoolTest");
    }

    @Test
    public void ensureClosed() throws Throwable {
        int initialConnection = getCurrentConnections();

        MariaDbPoolDataSource pool = new MariaDbPoolDataSource(connUri + "&maxPoolSize=10&minPoolSize=1");

        Connection connection = pool.getConnection();
        connection.isValid(10000);
        connection.close();

        assertTrue(getCurrentConnections() > initialConnection);

        //reuse IdleConnection
        connection = pool.getConnection();
        connection.isValid(10000);
        connection.close();

        assertTrue(getCurrentConnections() > initialConnection);
        pool.close();

        assertEquals(initialConnection, getCurrentConnections());
    }

    @Test
    public void wrongUrlHandling() throws SQLException {

        int initialConnection = getCurrentConnections();
        MariaDbPoolDataSource pool = new MariaDbPoolDataSource("jdbc:mariadb://unknownHost/db?user=wrong&maxPoolSize=10&connectTimeout=500");
        pool.initialize();
        long start = System.currentTimeMillis();
        Connection connection = null;
        try {
            connection = pool.getConnection();
            fail();
        } catch (SQLException sqle) {
            assertTrue("timeout does not correspond to option. Elapsed time:" + (System.currentTimeMillis() - start),
                    (System.currentTimeMillis() - start) >= 500 && (System.currentTimeMillis() - start) < 700);
            assertTrue(sqle.getMessage().contains("No connection available within the specified time (option 'connectTimeout': 500 ms)"));
        } finally {
            if (connection != null) connection.close();
        }
        pool.close();
    }



    //TODO check that threads are destroy when closing pool
}