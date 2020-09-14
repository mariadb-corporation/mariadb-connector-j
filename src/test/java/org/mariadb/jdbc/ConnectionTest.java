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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import org.junit.*;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

public class ConnectionTest extends BaseTest {

  /**
   * Initialisation.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("dummy", "a BLOB");
  }

  /** Conj-166. Connection error code must be thrown */
  @Test
  public void testAccessDeniedErrorCode() {
    try {
      DriverManager.getConnection(
          "jdbc:mariadb://"
              + ((hostname != null) ? hostname : "localhost")
              + ":"
              + port
              + "/"
              + database
              + "?user=foo");
      fail();
    } catch (SQLException e) {
      switch (e.getErrorCode()) {
        case (1524):
          // GSSAPI plugin not loaded
          assertTrue("HY000".equals(e.getSQLState()));
          break;

        case (1045):
          assertTrue("28000".equals(e.getSQLState()));
          break;

        case (1044):
          // mysql
          assertTrue("42000".equals(e.getSQLState()));
          break;

        default:
          e.printStackTrace();
          break;
      }
    }
  }

  /**
   * Conj-75 (corrected with CONJ-156) Needs permission java.sql.SQLPermission "abort" or will be
   * skipped.
   *
   * @throws SQLException exception
   */
  @Test
  public void abortTest() throws SQLException {
    try (Connection connection = setConnection()) {

      try (Statement stmt = connection.createStatement()) {

        SQLPermission sqlPermission = new SQLPermission("callAbort");
        SecurityManager securityManager = System.getSecurityManager();
        if (securityManager != null) {
          try {
            securityManager.checkPermission(sqlPermission);
          } catch (SecurityException se) {
            System.out.println("test 'abortTest' skipped  due to missing policy");
            return;
          }
        }

        Executor executor = Runnable::run;

        connection.abort(executor);
        assertTrue(connection.isClosed());
        try {
          stmt.executeQuery("SELECT 1");
          fail();
        } catch (SQLException sqle) {
          // normal exception
        }
      }
    }
  }

  @Test
  public void abortTestAlreadyClosed() throws SQLException {
    Connection connection = setConnection();
    connection.close();
    Executor executor = Runnable::run;
    connection.abort(executor);
  }

  @Test
  public void abortTestNoExecutor() {
    try {
      sharedConnection.abort(null);
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot abort the connection: null executor passed"));
    }
  }

  /**
   * Conj-121: implemented Connection.getNetworkTimeout and Connection.setNetworkTimeout.
   *
   * @throws SQLException exception
   */
  @Test
  public void networkTimeoutTest() throws SQLException {
    try (Connection connection = setConnection()) {

      int timeout = 1000;
      SQLPermission sqlPermission = new SQLPermission("setNetworkTimeout");
      SecurityManager securityManager = System.getSecurityManager();
      if (securityManager != null) {
        try {
          securityManager.checkPermission(sqlPermission);
        } catch (SecurityException se) {
          System.out.println("test 'setNetworkTimeout' skipped  due to missing policy");
          return;
        }
      }
      Executor executor = Runnable::run;
      try {
        connection.setNetworkTimeout(executor, timeout);
      } catch (SQLException sqlex) {
        sqlex.printStackTrace();
        fail(sqlex.getMessage());
      }

      connection.isValid(2);
      assertEquals(timeout, connection.getNetworkTimeout());

      try {
        int networkTimeout = connection.getNetworkTimeout();
        assertEquals(timeout, networkTimeout);
      } catch (SQLException sqlex) {
        sqlex.printStackTrace();
        fail(sqlex.getMessage());
      }
      try {
        connection.createStatement().execute("select sleep(2)");
        fail("Network timeout is " + timeout / 1000 + "sec, but slept for 2sec");
      } catch (SQLException sqlex) {
        assertTrue(connection.isClosed());
      }
    }
  }

  /** Conj-120 Fix Connection.isValid method. */
  @Test
  public void isValidShouldThrowExceptionWithNegativeTimeout() {
    try {
      sharedConnection.isValid(-1);
      fail("The above row should have thrown an SQLException");
    } catch (SQLException sqlex) {
      assertTrue(sqlex.getMessage().contains("negative"));
    }
  }

  /**
   * Conj-116: Make SQLException prettier when too large stream is sent to the server.
   *
   * @throws Throwable exception
   */
  @Test
  public void checkMaxAllowedPacket() throws Throwable {
    Statement statement = sharedConnection.createStatement();
    ResultSet rs = statement.executeQuery("show variables like 'max_allowed_packet'");
    assertTrue(rs.next());
    int maxAllowedPacket = rs.getInt(2);
    Assume.assumeTrue(maxAllowedPacket < 40_000_000);

    // Create a SQL stream bigger than maxAllowedPacket
    StringBuilder sb = new StringBuilder();
    String rowData = "('this is a dummy row values')";
    int rowsToWrite = (maxAllowedPacket / rowData.getBytes(StandardCharsets.UTF_8).length) + 1;
    try {
      for (int row = 1; row <= rowsToWrite; row++) {
        if (row >= 2) {
          sb.append(", ");
        }
        sb.append(rowData);
      }
      statement.executeUpdate("INSERT INTO dummy VALUES " + sb.toString());
      fail("The previous statement should throw an SQLException");
    } catch (OutOfMemoryError e) {
      System.out.println("skip test 'maxAllowedPackedExceptionIsPrettyTest' - not enough memory");
      Assume.assumeNoException(e);
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("max_allowed_packet"));
    } catch (Exception e) {
      fail("The previous statement should throw an SQLException not a general Exception");
    }

    statement.execute("select count(*) from dummy"); // check that the connection is still working

    // added in CONJ-151 to check the 2 different type of query implementation
    PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("INSERT INTO dummy VALUES (?)");
    try {
      byte[] arr = new byte[maxAllowedPacket + 1000];
      Arrays.fill(arr, (byte) 'a');
      preparedStatement.setBytes(1, arr);
      preparedStatement.addBatch();
      preparedStatement.executeBatch();
      fail("The previous statement should throw an SQLException");
    } catch (OutOfMemoryError e) {
      System.out.println(
          "skip second test 'maxAllowedPackedExceptionIsPrettyTest' - not enough memory");
      Assume.assumeNoException(e);
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("max_allowed_packet"));
    } catch (Exception e) {
      e.printStackTrace();
      fail("The previous statement should throw an SQLException not a general Exception");
    } finally {
      statement.execute("select count(*) from dummy"); // to check that connection is open
    }
  }

  @Test
  public void isValidTestWorkingConnection() throws SQLException {
    assertTrue(sharedConnection.isValid(0));
  }

  /**
   * CONJ-120 Fix Connection.isValid method
   *
   * @throws SQLException exception
   */
  @Test
  public void isValidClosedConnection() throws SQLException {
    try (Connection connection = setConnection()) {
      connection.close();
      boolean isValid = connection.isValid(0);
      assertFalse(isValid);
    }
  }

  /**
   * CONJ-120 Fix Connection.isValid method
   *
   * @throws SQLException exception
   * @throws InterruptedException exception
   */
  @Test
  public void isValidConnectionThatTimesOutByServer() throws SQLException, InterruptedException {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("set session wait_timeout=1");
        Thread.sleep(3000); // Wait for the server to kill the connection
        boolean isValid = connection.isValid(0);
        assertFalse(isValid);
      }
    }
  }

  /**
   * CONJ-363 : follow JDBC connection client infos rules.
   *
   * @throws Exception if any occur.
   */
  @Test
  public void testConnectionClientInfos() throws Exception {
    assertNull(sharedConnection.getClientInfo("ApplicationName"));
    assertNull(sharedConnection.getClientInfo("ClientUser"));
    assertNull(sharedConnection.getClientInfo("ClientHostname"));

    try {
      sharedConnection.getClientInfo("otherName");
      fail("Must have throw exception since name wasn't correct");
    } catch (SQLException sqlEx) {
      assertTrue(sqlEx.getMessage().contains("name must be"));
    }

    Properties properties = sharedConnection.getClientInfo();
    assertNull(properties.get("ApplicationName"));
    assertNull(properties.get("ClientUser"));
    assertNull(properties.get("ClientHostname"));

    sharedConnection.setClientInfo("ClientHostname", "testHostName");
    assertEquals("testHostName", sharedConnection.getClientInfo("ClientHostname"));
    properties = sharedConnection.getClientInfo();
    assertNull(properties.get("ApplicationName"));
    assertNull(properties.get("ClientUser"));
    assertEquals("testHostName", properties.get("ClientHostname"));

    sharedConnection.setClientInfo("ClientUser", "bbb");
    properties = sharedConnection.getClientInfo();
    assertNull(properties.get("ApplicationName"));
    assertEquals("bbb", properties.get("ClientUser"));
    assertEquals("testHostName", properties.get("ClientHostname"));

    sharedConnection.setClientInfo("ApplicationName", "ccc");
    properties = sharedConnection.getClientInfo();
    assertEquals("ccc", properties.get("ApplicationName"));
    assertEquals("bbb", properties.get("ClientUser"));
    assertEquals("testHostName", properties.get("ClientHostname"));

    sharedConnection.setClientInfo("ClientHostname", null);
    assertNull(sharedConnection.getClientInfo("ClientHostname"));

    sharedConnection.setClientInfo("ClientHostname", "");
    assertEquals("", sharedConnection.getClientInfo("ClientHostname"));

    properties = new Properties();
    properties.setProperty("ApplicationName", "test\\Driver");
    properties.setProperty("ClientUser", "test Client User");
    properties.setProperty("NotPermitted", "blabla");
    properties.setProperty("NotPermitted2", "blabla");

    try {
      sharedConnection.setClientInfo(properties);
    } catch (SQLClientInfoException sqle) {
      assertEquals(
          "setClientInfo errors : the following properties where not set :{NotPermitted,NotPermitted2}",
          sqle.getMessage());
      Map<String, ClientInfoStatus> failedProperties = sqle.getFailedProperties();
      assertTrue(failedProperties.containsKey("NotPermitted"));
      assertTrue(failedProperties.containsKey("NotPermitted2"));
      assertEquals(2, failedProperties.size());
    }

    assertEquals("test\\Driver", sharedConnection.getClientInfo("ApplicationName"));
    assertEquals("test Client User", sharedConnection.getClientInfo("ClientUser"));
    assertEquals(null, sharedConnection.getClientInfo("ClientHostname"));

    sharedConnection.setClientInfo("ClientUser", "otherValue");

    assertEquals("test\\Driver", sharedConnection.getClientInfo("ApplicationName"));
    assertEquals("otherValue", sharedConnection.getClientInfo("ClientUser"));
    assertEquals(null, sharedConnection.getClientInfo("ClientHostname"));

    try {
      sharedConnection.setClientInfo("NotPermitted", "otherValue");
      fail("Must have send an exception");
    } catch (SQLClientInfoException sqle) {
      assertEquals(
          "setClientInfo() parameters can only be \"ApplicationName\",\"ClientUser\" or \"ClientHostname\", "
              + "but was : NotPermitted",
          sqle.getMessage());
      Map<String, ClientInfoStatus> failedProperties = sqle.getFailedProperties();
      assertTrue(failedProperties.containsKey("NotPermitted"));
      assertEquals(1, failedProperties.size());
    }
  }

  @Test
  public void setClientError() throws SQLException {
    Connection connection = setConnection("");
    connection.close();
    try {
      connection.setClientInfo("ClientUser", "otherValue");
      fail();
    } catch (SQLClientInfoException e) {
      assertTrue(e.getMessage().contains("setClientInfo() is called on closed connection"));
    }
  }

  @Test
  public void retrieveCatalogTest() throws SQLException {
    final String db = sharedConnection.getCatalog();
    Statement stmt = sharedConnection.createStatement();

    stmt.execute("CREATE DATABASE gogogo");
    stmt.execute("USE gogogo");
    String db2 = sharedConnection.getCatalog();
    assertEquals("gogogo", db2);
    assertNotEquals(db, db2);
    stmt.execute("DROP DATABASE gogogo");

    String db3 = sharedConnection.getCatalog();
    assertNull(db3);
    stmt.execute("USE " + db);
  }

  @Test(timeout = 15_000L)
  public void testValidTimeout() throws Throwable {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = createProxyConnection(new Properties())) {

      assertTrue(connection.isValid(1)); // 1 second

      // ensuring to reactivate proxy
      Timer timer = new Timer();
      timer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              removeDelayProxy();
            }
          },
          1000);

      delayProxy(200);
      assertTrue(connection.isValid(1)); // 1 second
      Thread.sleep(2000);
    } finally {
      closeProxy();
    }
  }

  @Test
  public void testRst() throws Throwable {
    Assume.assumeFalse(sharedIsAurora());
    Properties props = new Properties();
    props.setProperty("enablePacketDebug", "true");
    props.setProperty("autoReconnect", "true");
    try (Connection connection = createProxyConnection(props)) {
      assertTrue(connection.isValid(1)); // 1 second
      forceCloseProxy();
      connection.createStatement().execute("SELECT 1");
      fail("must have failed");
    } catch (SQLException sqle) {
      assertTrue(sqle.getCause().getCause().getMessage().contains("send at -exchange:"));
    } finally {
      closeProxy();
    }
  }

  @Test(timeout = 15_000L)
  public void testValidFailedTimeout() throws Throwable {
    Properties properties = new Properties();
    properties.setProperty("usePipelineAuth", "false");
    try (Connection connection = createProxyConnection(properties)) {

      assertTrue(connection.isValid(1)); // 1 second

      // ensuring to reactivate proxy
      Timer timer = new Timer();
      timer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              removeDelayProxy();
            }
          },
          2000);

      delayProxy(1500);
      long start = System.currentTimeMillis();
      assertFalse(connection.isValid(1)); // 1 second
      assertTrue(System.currentTimeMillis() - start < 1250);
      Thread.sleep(5000);
    } finally {
      closeProxy();
    }
  }

  @Test
  public void standardClose() throws Throwable {
    Connection connection = setConnection();
    Statement stmt = connection.createStatement();
    stmt.setFetchSize(1);
    ResultSet rs = stmt.executeQuery("select * from information_schema.columns as c1");
    assertTrue(rs.next());
    connection.close();
    // must still work
    try {
      assertTrue(rs.next());
      fail();
    } catch (SQLException sqle) {
      assertTrue(sqle.getMessage().contains("Operation not permit on a closed resultSet"));
    }
  }

  @Test(timeout = 5_000L)
  public void abortClose() throws Throwable {
    Connection connection = setConnection();
    Statement stmt = connection.createStatement();
    stmt.setFetchSize(1);
    ResultSet rs =
        stmt.executeQuery(
            "select * from information_schema.columns as c1, "
                + "information_schema.tables, information_schema.tables as t2");
    assertTrue(rs.next());
    connection.abort(SchedulerServiceProviderHolder.getBulkScheduler());
    // must still work

    Thread.sleep(20);
    try {
      assertTrue(rs.next());
      fail();
    } catch (SQLException sqle) {
      assertTrue(sqle.getMessage().contains("Operation not permit on a closed resultSet"));
    }
  }

  @Test(timeout = 5_000L)
  public void verificationAbort() throws Throwable {
    Timer timer = new Timer();
    try (Connection connection = setConnection()) {
      timer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              try {
                connection.abort(SchedulerServiceProviderHolder.getBulkScheduler());
              } catch (SQLException sqle) {
                fail(sqle.getMessage());
              }
            }
          },
          10);

      Statement stmt = connection.createStatement();
      try {
        stmt.executeQuery(
            "select * from information_schema.columns as c1,  information_schema.tables, information_schema.tables as t2");
      } catch (SQLException sqle) {
        assertTrue(sqle.getMessage().contains("Connection has explicitly been closed/aborted"));
      }
    }
  }

  @Test
  public void verificationEd25519AuthPlugin() throws Throwable {
    Assume.assumeTrue(
        System.getenv("MAXSCALE_TEST_DISABLE") == null && System.getenv("SKYSQL") == null);
    Assume.assumeTrue(isMariadbServer() && minVersion(10, 2));
    Statement stmt = sharedConnection.createStatement();

    try {
      stmt.execute("INSTALL SONAME 'auth_ed25519'");
    } catch (SQLException sqle) {
      throw new AssumptionViolatedException("server doesn't have ed25519 plugin, cancelling test");
    }
    try {
      if (minVersion(10, 4)) {
        stmt.execute(
            "CREATE USER verificationEd25519AuthPlugin IDENTIFIED "
                + "VIA ed25519 USING PASSWORD('MySup8%rPassw@ord')");
      } else {
        stmt.execute(
            "CREATE USER verificationEd25519AuthPlugin IDENTIFIED "
                + "VIA ed25519 USING '6aW9C7ENlasUfymtfMvMZZtnkCVlcb1ssxOLJ0kj/AA'");
      }
    } catch (SQLException sqle) {
      // already existing
    }
    stmt.execute("GRANT SELECT on " + database + ".* to verificationEd25519AuthPlugin");

    String url =
        "jdbc:mariadb://"
            + hostname
            + ((port == 0) ? "" : ":" + port)
            + "/"
            + database
            + "?user=verificationEd25519AuthPlugin&password=MySup8%rPassw@ord";

    try (Connection connection = openNewConnection(url)) {
      // must have succeed
    }
    stmt.execute("drop user verificationEd25519AuthPlugin");
  }

  private void initializeDns(String host) {
    try {
      InetAddress.getByName(host);
      fail();
    } catch (UnknownHostException e) {
      // normal error
    }
  }

  @Test
  public void loopSleepTest() {
    // appveyor vm are very slow, cannot compare time
    Assume.assumeTrue(System.getenv("APPVEYOR") == null);

    // initialize DNS to avoid having wrong timeout
    initializeDns("host1");
    initializeDns("host2");
    initializeDns("host1.555-rds.amazonaws.com");

    // failover
    checkConnection(
        "jdbc:mariadb:failover://host1,host2/testj?user=root&retriesAllDown=20&connectTimeout=20",
        2000,
        3200);
    // replication
    checkConnection(
        "jdbc:mariadb:replication://host1,host2/testj?user=root&retriesAllDown=20&connectTimeout=20",
        2000,
        3200);
    // aurora - no cluster end point
    checkConnection(
        "jdbc:mariadb:aurora://host1,host2/testj?user=root&retriesAllDown=20&connectTimeout=20",
        2000,
        3200);
    // aurora - using cluster end point
    checkConnection(
        "jdbc:mariadb:aurora://host1.555-rds.amazonaws.com/testj?user=root&retriesAllDown=20&connectTimeout=20",
        4500,
        10000);
  }

  private void checkConnection(String conUrl, int min, int max) {
    long start = System.currentTimeMillis();
    try (Connection connection = DriverManager.getConnection(conUrl)) {
      fail();
    } catch (SQLException e) {
      // excepted exception
      // since retriesAllDown is = 20 , that means 10 entire loop with 250ms sleep
      // first loop has not sleep, last too, so 8 * 250 = 2s
      System.out.println(System.currentTimeMillis() - start);
      assertTrue(System.currentTimeMillis() - start > min);
      assertTrue(System.currentTimeMillis() - start < max);
    }
  }

  @Test
  public void slaveDownConnection() throws SQLException {
    Assume.assumeTrue(System.getenv("SKYSQL") == null);
    String url =
        "jdbc:mariadb:replication://"
            + hostname
            + ((port == 0) ? "" : ":" + port)
            + ","
            + hostname
            + ":8888"
            + "/"
            + database
            + "?user="
            + username
            + ((password != null) ? "&password=" + password : "")
            + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
            + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
            + "&retriesAllDown=10&allowMasterDownConnection&connectTimeout=100&socketTimeout=100";
    try (Connection connection = DriverManager.getConnection(url)) {
      Assert.assertFalse(connection.isReadOnly());
      connection.isValid(0);
      try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1")) {
        preparedStatement.executeQuery();
      }
      connection.setReadOnly(true);
      try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1")) {
        preparedStatement.executeQuery();
      }
    }
  }

  @Test
  public void multiAuthPlugin() throws Throwable {
    Assume.assumeTrue(
        System.getenv("MAXSCALE_TEST_DISABLE") == null && System.getenv("SKYSQL") == null);
    Assume.assumeTrue(isMariadbServer() && minVersion(10, 4, 2));
    Statement stmt = sharedConnection.createStatement();
    try {
      stmt.execute("INSTALL SONAME 'auth_ed25519'");
    } catch (SQLException sqle) {
      throw new AssumptionViolatedException("server doesn't have ed25519 plugin, cancelling test");
    }

    stmt.execute("drop user IF EXISTS mysqltest1@'%'");
    try {
      stmt.execute(
          "CREATE USER mysqltest1@'%' IDENTIFIED "
              + "VIA ed25519 as password('!Passw0rd3') "
              + " OR mysql_native_password as password('!Passw0rd3Works')");

    } catch (SQLException sqle) {
      // already existing
      sqle.printStackTrace();
    }
    stmt.execute("GRANT SELECT on " + database + ".* to mysqltest1@'%'");

    try (Connection connection =
        openNewConnection(
            "jdbc:mariadb://"
                + hostname
                + ((port == 0) ? "" : ":" + port)
                + "/"
                + database
                + "?user=mysqltest1&password=!Passw0rd3")) {
      // must have succeed
    }

    try (Connection connection =
        openNewConnection(
            "jdbc:mariadb://"
                + hostname
                + ((port == 0) ? "" : ":" + port)
                + "/"
                + database
                + "?user=mysqltest1&password=!Passw0rd3Works")) {
      // must have succeed
    }

    stmt.execute("drop user mysqltest1@'%'");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void quoteIdentifier() {
    assertEquals("`te``st`", MariaDbConnection.quoteIdentifier("te`st"));
    assertEquals("test", MariaDbConnection.unquoteIdentifier("`test`"));
    assertEquals("te`st", MariaDbConnection.unquoteIdentifier("`te``st`"));
    assertEquals("te`st", MariaDbConnection.unquoteIdentifier("te`st"));
  }

  @Test
  public void connectionUnexpectedClose() throws SQLException {
    Assume.assumeTrue(System.getenv("SKYSQL") == null);
    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:mariadb:failover//"
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + "/"
                + database
                + "?user="
                + username
                + ((password != null) ? "&password=" + password : "")
                + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
                + "&socketTimeout=1000&useServerPrepStmts=true")) {
      Statement stmt = connection.createStatement();
      try {
        stmt.executeQuery("KILL CONNECTION_ID()");
        fail();
      } catch (SQLException e) {
        // eat
      }
      try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1")) {
        preparedStatement.execute();
      }
    }
  }

  @Test
  public void prepareStatementCols() throws SQLException {
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT 1", new int[] {})) {
      preparedStatement.execute();
    }
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT 1", new String[] {})) {
      preparedStatement.execute();
    }
    try {
      sharedConnection.prepareStatement(null);
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("SQL value can not be NULL"));
    }
  }

  @Test
  public void nativeSql() throws SQLException {
    assertEquals(
        "select timestampdiff(HOUR, convert('SQL_', INTEGER))",
        sharedConnection.nativeSQL(
            "select {fn timestampdiff(SQL_TSI_HOUR, {fn convert('SQL_', SQL_INTEGER)})}"));
  }

  @Test
  public void setReadonlyError() throws SQLException {
    Assume.assumeTrue(System.getenv("SKYSQL") == null);
    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:mariadb:replication://"
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + ","
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + "/"
                + database
                + "?user="
                + username
                + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
                + ((password != null) ? "&password=" + password : ""))) {
      connection.setReadOnly(true);
      long threadId = ((MariaDbConnection) connection).getServerThreadId();
      connection.setReadOnly(false);
      Statement stmt = connection.createStatement();
      stmt.executeQuery("KILL " + threadId);
      try {
        stmt.executeQuery("KILL CONNECTION_ID()");
        fail();
      } catch (SQLException e) {
        // eat
      }
      connection.setReadOnly(true);
    }
  }

  @Test
  public void testWarnings() throws SQLException {
    Assume.assumeTrue(isMariadbServer());
    Statement stmt = sharedConnection.createStatement();
    stmt.executeQuery("select now() = 1");
    SQLWarning warning = sharedConnection.getWarnings();
    assertTrue(warning.getMessage().contains("ncorrect datetime value: '1'"));
    sharedConnection.clearWarnings();
    assertNull(sharedConnection.getWarnings());
  }

  @Test
  public void typeMap() throws SQLException {
    try {
      sharedConnection.setTypeMap(null);
      fail();
    } catch (SQLFeatureNotSupportedException e) {
      assertTrue(e.getMessage().contains("TypeMap are not supported"));
    }

    assertTrue(sharedConnection.getTypeMap().isEmpty());
  }

  @Test
  public void getHoldability() throws SQLException {
    sharedConnection.setHoldability(10_000);
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, sharedConnection.getHoldability());
  }

  @Test
  public void notSupported() throws SQLException {
    try {
      sharedConnection.createSQLXML();
      fail();
    } catch (SQLFeatureNotSupportedException e) {
      assertTrue(e.getMessage().contains("SQLXML type is not supported"));
    }

    try {
      sharedConnection.createArrayOf("", null);
      fail();
    } catch (SQLFeatureNotSupportedException e) {
      assertTrue(e.getMessage().contains("Array type is not supported"));
    }

    try {
      sharedConnection.createStruct("", null);
      fail();
    } catch (SQLFeatureNotSupportedException e) {
      assertTrue(e.getMessage().contains("Struct type is not supported"));
    }
    sharedConnection.setSchema("bbb");
    assertNull(sharedConnection.getSchema());
  }

  @Test
  public void unwrapp() throws Throwable {
    assertTrue(sharedConnection.isWrapperFor(MariaDbConnection.class));
    MariaDbConnection cc = sharedConnection.unwrap(MariaDbConnection.class);
  }

  @Test
  public void setClientNotConnectError() throws SQLException {
    Assume.assumeTrue(
        System.getenv("MAXSCALE_TEST_DISABLE") == null && System.getenv("SKYSQL") == null);
    // only mariadb return a specific error when connection has explicitly been killed
    Assume.assumeTrue(isMariadbServer());

    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:mariadb:replication://"
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + ","
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + "/"
                + database
                + "?log&user="
                + username
                + ((password != null) ? "&password=" + password : "")
                + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                + ((options.serverSslCert != null)
                    ? "&serverSslCert=" + options.serverSslCert
                    : ""))) {
      connection.setReadOnly(true);
      long threadId = ((MariaDbConnection) connection).getServerThreadId();
      connection.setReadOnly(false);
      Statement stmt = connection.createStatement();
      stmt.executeQuery("KILL " + threadId);
      try {
        stmt.executeQuery("KILL CONNECTION_ID()");
        fail();
      } catch (SQLException e) {
        // eat
      }
      connection.setClientInfo("ClientUser", "otherValue");
    }
  }

  @Test
  public void testNetworkTimeoutError() throws SQLException {
    Connection connection = setConnection("&socketTimeout=10000");
    assertEquals(10_000, connection.getNetworkTimeout());
    connection.setNetworkTimeout(null, 5_000);

    try {
      connection.setNetworkTimeout(null, -5_000);
    } catch (SQLException e) {
      assertTrue(
          e.getMessage()
              .contains("Connection.setNetworkTimeout cannot be called with a negative timeout"));
    }
    connection.close();
    try {
      connection.setNetworkTimeout(null, 3_000);
    } catch (SQLException e) {
      assertTrue(
          e.getMessage()
              .contains("Connection.setNetworkTimeout cannot be called on a closed connection"));
    }

    try (Connection connection2 = setConnection("&socketTimeout=10000")) {
      assertEquals(10_000, connection2.getNetworkTimeout());
    }
  }

  @Test
  public void readOnly() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("CREATE TABLE testReadOnly(id int)");
    sharedConnection.setAutoCommit(false);

    sharedConnection.setReadOnly(true);
    stmt.execute("INSERT INTO testReadOnly values (1)");
    sharedConnection.commit();

    Connection connection = setConnection("&assureReadOnly=true");
    stmt = connection.createStatement();

    connection.setAutoCommit(false);
    connection.setReadOnly(true);
    try {
      stmt.execute("INSERT INTO testReadOnly values (2)");
      fail();
    } catch (SQLException sqle) {
      assertTrue(sqle.getMessage().contains("Cannot execute statement in a READ ONLY transaction"));
    }
    connection.setReadOnly(false);
    stmt.execute("DROP TABLE testReadOnly");
  }

  @Test
  public void connectionAttributes() throws SQLException {

    try (MariaDbConnection conn =
        (MariaDbConnection) setConnection("&connectionAttributes=test:test1")) {
      Statement stmt = conn.createStatement();
      ResultSet rs1 = stmt.executeQuery("SELECT @@performance_schema");
      rs1.next();
      if ("1".equals(rs1.getString(1))) {
        ResultSet rs =
            stmt.executeQuery(
                "SELECT * from performance_schema.session_connect_attrs where processlist_id="
                    + conn.getServerThreadId()
                    + " AND ATTR_NAME='test'");
        while (rs.next()) {
          assertEquals("test1", rs.getString("ATTR_VALUE"));
        }
      }
    }
  }
}
