// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.SingleStorePoolDataSource;
import com.singlestore.jdbc.pool.PoolThreadFactory;
import com.singlestore.jdbc.pool.Pools;
import java.lang.management.ManagementFactory;
import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import org.junit.jupiter.api.*;

public class PoolDataSourceTest extends Common {

  /** Initialisation. */
  @BeforeAll
  public static void beforeClassDataSourceTest() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();

    stmt.execute(
        "GRANT SELECT ON "
            + sharedConn.getCatalog()
            + ".* TO 'poolUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");

    stmt.execute(
        "CREATE TABLE testResetRollback(id int not null primary key auto_increment, test varchar(20))");
    stmt.execute("FLUSH TABLES");
    stmt.execute("FLUSH PRIVILEGES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    try (Statement stmt = sharedConn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS testResetRollback");
    }
  }

  @Test
  public void testDataSource() throws SQLException {
    try (SingleStorePoolDataSource ds = new SingleStorePoolDataSource(mDefUrl)) {
      try (Connection connection = ds.getConnection()) {
        assertEquals(connection.isValid(0), true);
      }

      try (Connection connection = ds.getConnection("poolUser", "!Passw0rd3Works")) {
        assertEquals(connection.isValid(0), true);
      }

      PooledConnection poolCon = ds.getPooledConnection();
      assertEquals(poolCon.getConnection().isValid(0), true);
      poolCon.close();
      poolCon = ds.getPooledConnection("poolUser", "!Passw0rd3Works");
      assertEquals(poolCon.getConnection().isValid(0), true);
      poolCon.close();
    }
  }

  @Test
  public void testResetDatabase() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        statement.execute("CREATE DATABASE IF NOT EXISTS testingReset");
        connection.setCatalog("testingReset");
      }

      try (Connection connection = pool.getConnection()) {
        assertEquals(sharedConn.getCatalog(), connection.getCatalog());
        Statement statement = connection.createStatement();
        statement.execute("DROP DATABASE testingReset");
      }
    }
  }

  @Test
  public void testResetSessionVariable() throws SQLException {
    testResetSessionVariable(false);
    if (minVersion(7, 3, 0)) {
      testResetSessionVariable(true);
    }
  }

  private void testResetSessionVariable(boolean useResetConnection) throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl + "&maxPoolSize=1&useResetConnection=" + useResetConnection)) {

      long nowMillis;
      int initialWaitTimeout;

      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();

        nowMillis = getNowTime(statement);
        initialWaitTimeout = getWaitTimeout(statement);

        statement.execute(
            "SET @@timestamp=UNIX_TIMESTAMP('1970-10-01 01:00:00'), @@wait_timeout=2000");
        long newNowMillis = getNowTime(statement);
        int waitTimeout = getWaitTimeout(statement);

        assertTrue(nowMillis - newNowMillis > 23_587_200_000L);
        assertEquals(2_000, waitTimeout);
      }

      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();

        long newNowMillis = getNowTime(statement);
        int waitTimeout = getWaitTimeout(statement);

        if (useResetConnection) {
          assertTrue(nowMillis - newNowMillis < 10L);
          assertEquals(initialWaitTimeout, waitTimeout);
        } else {
          assertTrue(nowMillis - newNowMillis > 23_587_200_000L);
          assertEquals(2_000, waitTimeout);
        }
      }
    }
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
    // S2 < 7.3 does not support user-defined variables
    Assumptions.assumeTrue(minVersion(7, 3, 0));
    testResetUserVariable(false);
    testResetUserVariable(false);
    if (minVersion(7, 5, 0)) {
      testResetUserVariable(true);
      testResetUserVariable(true);
    }
  }

  private void testResetUserVariable(boolean useResetConnection) throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl + "&maxPoolSize=1&useResetConnection=" + useResetConnection)) {
      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        assertUnknown(statement);

        statement.execute("SELECT '123' INTO @str");

        assertEquals("123", getUserVariableStr(statement));
      }

      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        if (useResetConnection) {
          assertUnknown(statement);
        } else {
          assertEquals("123", getUserVariableStr(statement));
        }
      }
    }
  }

  private void assertUnknown(Statement statement) {
    assertThrowsContains(
        SQLException.class,
        () -> getUserVariableStr(statement),
        "Unknown user-defined variable @`str`");
  }

  private String getUserVariableStr(Statement statement) throws SQLException {
    ResultSet rs = statement.executeQuery("SELECT @str");
    assertTrue(rs.next());
    return rs.getString(1);
  }

  @Test
  public void testNetworkTimeout() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1&socketTimeout=10000")) {
      try (Connection connection = pool.getConnection()) {
        assertEquals(10_000, connection.getNetworkTimeout());
        connection.setNetworkTimeout(null, 5_000);
      }

      try (Connection connection = pool.getConnection()) {
        assertEquals(10_000, connection.getNetworkTimeout());
      }
    }
  }

  @Test
  public void testResetReadOnly() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        assertFalse(connection.isReadOnly());
        connection.setReadOnly(true);
        assertTrue(connection.isReadOnly());
      }

      try (Connection connection = pool.getConnection()) {
        assertFalse(connection.isReadOnly());
      }
    }
  }

  @Test
  public void testResetAutoCommit() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        assertTrue(connection.getAutoCommit());
        connection.setAutoCommit(false);
        assertFalse(connection.getAutoCommit());
      }

      try (Connection connection = pool.getConnection()) {
        assertTrue(connection.getAutoCommit());
      }
    }
  }

  // TODO: PLAT-5847
  @Disabled
  @Test
  public void testResetAutoCommitOption() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl + "&maxPoolSize=1&autocommit=false&poolName=PoolTest")) {
      assertTrue(pool.getPoolName().startsWith("PoolTest-"));
      try (Connection connection = pool.getConnection()) {
        // This may or may not be a bug, but seems weird to me:
        // we don't actually set parameters provided through config like autocommit
        // when creating new connection. However, we do set them to config values
        // when resetting the connection. So in the case when value provided through the config
        // does not equal default (&autocommit=false while S2 has autocommit=true), we get different
        // values
        // before and after connection reset. Same thing for transactionIsolation
        assertFalse(connection.getAutoCommit());
        connection.setAutoCommit(false);
        assertFalse(connection.getAutoCommit());
      }

      try (Connection connection = pool.getConnection()) {
        assertFalse(connection.getAutoCommit());
      }
    }
  }

  @Test
  public void testResetTransactionIsolation() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1")) {

      try (Connection connection = pool.getConnection()) {
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
      }

      try (Connection connection = pool.getConnection()) {
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
      }
    }
  }

  @Test
  public void testJmx() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.singlestore.jdbc.pool:type=PoolTestJmx-*");
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl + "&maxPoolSize=5&minPoolSize=0&poolName=PoolTestJmx")) {
      try (Connection connection = pool.getConnection()) {
        connection.isValid(1);
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        checkJmxInfo(server, name, 1, 1, 0, 0);

        try (Connection connection2 = pool.getConnection()) {
          connection2.isValid(1);
          checkJmxInfo(server, name, 2, 2, 0, 0);
        }
        checkJmxInfo(server, name, 1, 2, 1, 0);
      }
    }
  }

  @Test
  public void testNoMinConnection() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.singlestore.jdbc.pool:type=testNoMinConnection-*");
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=5&poolName=testNoMinConnection")) {
      try (Connection connection = pool.getConnection()) {
        connection.isValid(1);
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        // wait to ensure pool has time to create 5 connections
        try {
          Thread.sleep(500);
        } catch (InterruptedException interruptEx) {
          // eat
        }

        checkJmxInfo(server, name, 1, 5, 4, 0);

        try (Connection connection2 = pool.getConnection()) {
          connection2.isValid(1);
          checkJmxInfo(server, name, 2, 5, 3, 0);
        }
        checkJmxInfo(server, name, 1, 5, 4, 0);
      }
    }
  }

  @Test
  public void testIdleTimeout() throws Throwable {
    // appveyor is so slow wait time are not relevant.
    Assumptions.assumeTrue(System.getenv("APPVEYOR_BUILD_WORKER_IMAGE") == null);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.singlestore.jdbc.pool:type=testIdleTimeout-*");
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl
                + "&maxPoolSize=5&minPoolSize=3&poolName=testIdleTimeout&testMinRemovalDelay=50&maxIdleTime=100")) {
      // wait to ensure pool has time to create 3 connections
      Thread.sleep(1_000);

      Set<ObjectName> objectNames = server.queryNames(filter, null);
      ObjectName name = objectNames.iterator().next();
      checkJmxInfo(server, name, 0, 3, 3, 0);

      List<Long> initialThreadIds = pool.testGetConnectionIdleThreadIds();
      Thread.sleep(200);

      // must still have 3 connections, but must be other ones
      checkJmxInfo(server, name, 0, 3, 3, 0);
    }
  }

  @Test
  public void testMinConnection() throws Throwable {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.singlestore.jdbc.pool:type=testMinConnection-*");
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl
                + "&maxPoolSize=5&minPoolSize=3&poolName=testMinConnection&testMinRemovalDelay=30&maxIdleTime=100")) {
      try (Connection connection = pool.getConnection()) {
        connection.isValid(1);
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        // to ensure pool has time to create minimal connection number
        Thread.sleep(200);

        checkJmxInfo(server, name, 1, 3, 2, 0);

        try (Connection connection2 = pool.getConnection()) {
          connection2.isValid(1);
          checkJmxInfo(server, name, 2, 3, 1, 0);
        }
        checkJmxInfo(server, name, 1, 3, 2, 0);
      }
    }
  }

  private void checkJmxInfo(
      MBeanServer server,
      ObjectName name,
      long expectedActive,
      long expectedTotal,
      long expectedIdle,
      long expectedRequest)
      throws Exception {

    assertEquals(
        expectedActive, ((Long) server.getAttribute(name, "ActiveConnections")).longValue());
    assertEquals(expectedTotal, ((Long) server.getAttribute(name, "TotalConnections")).longValue());
    assertEquals(expectedIdle, ((Long) server.getAttribute(name, "IdleConnections")).longValue());
    assertEquals(
        expectedRequest, ((Long) server.getAttribute(name, "ConnectionRequests")).longValue());
  }

  @Test
  public void testJmxDisable() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.singlestore.jdbc.pool:type=PoolTest-*");
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl + "&maxPoolSize=2&registerJmxPool=false&poolName=PoolTest")) {
      try (Connection connection = pool.getConnection()) {
        connection.isValid(1);
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(0, objectNames.size());
      }
    }
  }

  @Test
  public void testResetRollback() throws SQLException {
    sharedConn.createStatement().execute("FLUSH TABLES");
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("INSERT INTO testResetRollback (test) VALUES ('heja')");
        connection.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO testResetRollback (test) VALUES ('japp')");
      }

      try (Connection connection = pool.getConnection()) {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM testResetRollback");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  public void ensureUsingPool() throws Exception {
    ThreadPoolExecutor connectionAppender =
        new ThreadPoolExecutor(
            50,
            5000,
            10,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(5000),
            new PoolThreadFactory("testPool"));

    final long start = System.currentTimeMillis();
    Set<Integer> threadIds = new HashSet<>();
    for (int i = 0; i < 500; i++) {
      connectionAppender.execute(
          () -> {
            try (Connection connection =
                DriverManager.getConnection(
                    mDefUrl + "&pool&staticGlobal&poolName=PoolEnsureUsingPool&log=true")) {
              Statement stmt = connection.createStatement();
              ResultSet rs = stmt.executeQuery("SELECT CONNECTION_ID()");
              rs.next();
              Integer connectionId = rs.getInt(1);
              threadIds.add(connectionId);
              stmt.execute("SELECT * FROM INFORMATION_SCHEMA.USERS");

            } catch (SQLException e) {
              e.printStackTrace();
            }
          });
    }
    connectionAppender.shutdown();
    connectionAppender.awaitTermination(30, TimeUnit.SECONDS);
    assertTrue(threadIds.size() <= 9, "connection ids must be less than 9 : " + threadIds.size());
    Pools.close("PoolTest");
  }

  @Test
  public void ensureClosed() throws Throwable {
    Thread.sleep(500); // ensure that previous close are effective
    int initialConnection = getCurrentConnections();

    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=10&minPoolSize=1")) {

      try (Connection connection = pool.getConnection()) {
        connection.isValid(10_000);
      }

      assertTrue(getCurrentConnections() > initialConnection);

      // reuse IdleConnection
      try (Connection connection = pool.getConnection()) {
        connection.isValid(10_000);
      }

      Thread.sleep(500);
      assertTrue(getCurrentConnections() > initialConnection);
    }
    Thread.sleep(2000); // ensure that previous close are effective
    assertEquals(initialConnection, getCurrentConnections());
  }

  @Test
  public void wrongUrlHandling() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            "jdbc:singlestore://unknownHost/db?user=wrong&maxPoolSize=10&connectTimeout=500")) {
      long start = System.currentTimeMillis();
      try {
        pool.getConnection();
        fail();
      } catch (SQLException sqle) {
        assertTrue(
            (System.currentTimeMillis() - start) >= 500
                && (System.currentTimeMillis() - start) < 800,
            "timeout does not correspond to option. Elapsed time:"
                + (System.currentTimeMillis() - start));
        assertTrue(
            sqle.getMessage()
                .contains(
                    "No connection available within the specified time (option 'connectTimeout': 500 ms)"));
      }
    }
  }

  @Test
  public void testPrepareReset() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl + "&maxPoolSize=1&useServerPrepStmts=true&useResetConnection")) {
      try (Connection connection = pool.getConnection()) {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT ?");
        preparedStatement.setString(1, "1");
        preparedStatement.execute();
      }

      try (Connection connection = pool.getConnection()) {
        // must re-prepare
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT ?");
        preparedStatement.setString(1, "1");
        preparedStatement.execute();
      }
    }
  }

  /**
   * List current connections to server.
   *
   * @return number of thread connected.
   * @throws SQLException if queries failed
   */
  public static int getCurrentConnections() {
    try {
      Statement stmt = sharedConn.createStatement();
      ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Threads_connected'");
      assertTrue(rs.next());
      return rs.getInt(2);

    } catch (SQLException e) {
      return -1;
    }
  }

  @Test
  public void poolWithUser() throws SQLException {
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1&poolName=myPool")) {
      long threadId = 0;
      try (Connection conn = pool.getConnection()) {
        conn.isValid(1);
        threadId = ((com.singlestore.jdbc.Connection) conn).getThreadId();
      }

      try (Connection conn = pool.getConnection(user, password)) {
        conn.isValid(1);
        assertEquals(threadId, ((com.singlestore.jdbc.Connection) conn).getThreadId());
      }
      try (Connection conn = pool.getConnection("poolUser", "!Passw0rd3Works")) {
        conn.isValid(1);
        assertNotEquals(threadId, ((com.singlestore.jdbc.Connection) conn).getThreadId());
      }
    }
  }

  @Test
  public void various() throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> new SingleStorePoolDataSource("jdbc:notSingleStore"),
        "Wrong SingleStoreDB url");
    try (SingleStorePoolDataSource pool =
        new SingleStorePoolDataSource(
            mDefUrl + "&maxPoolSize=1&poolName=myPool&connectTimeout=2000")) {
      assertNotNull(pool.unwrap(com.singlestore.jdbc.SingleStorePoolDataSource.class));
      assertNotNull(pool.unwrap(ConnectionPoolDataSource.class));
      assertThrowsContains(
          SQLException.class,
          () -> pool.unwrap(String.class),
          "Datasource is not a wrapper for java.lang.String");
      assertTrue(pool.isWrapperFor(com.singlestore.jdbc.SingleStorePoolDataSource.class));
      assertTrue(pool.isWrapperFor(ConnectionPoolDataSource.class));
      assertFalse(pool.isWrapperFor(String.class));
      pool.setLogWriter(null);
      assertNull(pool.getLogWriter());
      assertNull(pool.getParentLogger());
      assertEquals(2, pool.getLoginTimeout());
      pool.setLoginTimeout(4);
      assertEquals(4, pool.getLoginTimeout());
    }
  }

  @Test
  public void pools() throws SQLException {
    // ensure all are closed
    Pools.close();
    Pools.close(null);
    new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1&poolName=myPool");
    Pools.close("myPool");
    new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1&poolName=myPool");
    Pools.close();
  }
}
