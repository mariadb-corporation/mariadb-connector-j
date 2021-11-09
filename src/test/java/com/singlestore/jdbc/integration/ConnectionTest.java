// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.*;
import com.singlestore.jdbc.integration.util.SocketFactoryTest;
import com.singlestore.jdbc.util.constants.Capabilities;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.*;

@DisplayName("Connection Test")
public class ConnectionTest extends Common {

  @Test
  public void isValid() throws SQLException {
    Connection sharedConn = DriverManager.getConnection(mDefUrl);
    assertTrue(sharedConn.isValid(2000));
    sharedConn.close();
    assertFalse(sharedConn.isValid(2000));
  }

  @Test
  void isValidWrongValue() {
    try {
      sharedConn.isValid(-2000);
      fail("most have thrown an error");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("the value supplied for timeout is negative"));
    }
  }

  @Test
  void socketTimeout() throws SQLException {
    try (Connection con = createCon("&socketTimeout=50")) {
      assertEquals(50, con.getNetworkTimeout());
      Statement stmt = con.createStatement();
      stmt.execute("SELECT 1");
      assertThrowsContains(SQLException.class, () -> stmt.execute("SELECT SLEEP(0.1)"), "");
    }

    try (Connection con = createCon("&socketTimeout=500")) {
      assertEquals(500, con.getNetworkTimeout());
      Statement stmt = con.createStatement();
      stmt.execute("SELECT SLEEP(0.1)");
      assertThrowsContains(SQLException.class, () -> stmt.execute("SELECT SLEEP(1)"), "");
    }

    try (Connection con = createCon("&socketTimeout=0")) {
      assertEquals(0, con.getNetworkTimeout());
      Statement stmt = con.createStatement();
      stmt.execute("SELECT SLEEP(0.5)");
    }
  }

  @Test
  public void autoCommit() throws SQLException {
    Connection con = DriverManager.getConnection(mDefUrl);
    assertTrue(con.getAutoCommit());
    con.setAutoCommit(false);
    assertFalse(con.getAutoCommit());
    con.setAutoCommit(false);
    assertFalse(con.getAutoCommit());
    con.setAutoCommit(true);
    assertTrue(con.getAutoCommit());
    con.setAutoCommit(true);
    assertTrue(con.getAutoCommit());
    Statement stmt = con.createStatement();
    stmt.execute("SET autocommit=false");
    assertFalse(con.getAutoCommit());
    con.close();
  }

  @Test
  public void nativeSQL() throws SQLException {
    String[] inputs =
        new String[] {
          "select {fn TIMESTAMPDIFF ( SQL_TSI_HOUR, {fn convert('SQL_', SQL_INTEGER)})}, {fn TIMESTAMPDIFF (HOUR, {fn convert  ('sQL_'   , SQL_INTEGER)})}",
          "{call foo({fn now()})} //end",
          "{call foo({fn '{' now()} /* {test}# \"'\" */) \"\\\"'#\" '\"\\''} #{test2}",
          "{  call foo({fn now()})}",
          "{\r\ncall foo({fn now()})}",
          "{\r\n  call foo({fn now()})}",
          "{call foo(/*{fn now()}*/)}",
          "{CALL foo({fn now() /* -- * */ -- test \n })}",
          "{?=call foo({fn now()})}",
          "SELECT 'David_' LIKE 'David|_' {escape '|'}",
          "select {fn dayname ({fn abs({fn now()})})}",
          "{d '1997-05-24'}",
          "{d'1997-05-24'}",
          "{\nt'10:30:29'}",
          "{t '10:30:29'}",
          "{t'10:30:29'}",
          "{ts '1997-05-24 10:30:29.123'}",
          "{ts'1997-05-24 10:30:29.123'}",
          "'{string data with { or } will not be altered'",
          "`{string data with { or } will not be altered`",
          "--  Also note that you can safely include { and } in comments",
          "SELECT * FROM {oj TABLE1 LEFT OUTER JOIN TABLE2 ON DEPT_NO = 003420930}"
        };
    String[] outputs =
        new String[] {
          "select TIMESTAMPDIFF ( HOUR, convert('SQL_', INTEGER)), TIMESTAMPDIFF (HOUR, convert  ('sQL_'   , INTEGER))",
          "call foo(now()) //end",
          "call foo('{' now() /* {test}# \"'\" */) \"\\\"'#\" '\"\\'' #{test2}",
          "call foo(now())",
          "call foo(now())",
          "call foo(now())",
          "call foo(/*{fn now()}*/)",
          "CALL foo(now() /* -- * */ -- test \n )",
          "?=call foo(now())",
          "SELECT 'David_' LIKE 'David|_' escape '|'",
          "select dayname (abs(now()))",
          "'1997-05-24'",
          "'1997-05-24'",
          "'10:30:29'",
          "'10:30:29'",
          "'10:30:29'",
          "'1997-05-24 10:30:29.123'",
          "'1997-05-24 10:30:29.123'",
          "'{string data with { or } will not be altered'",
          "`{string data with { or } will not be altered`",
          "--  Also note that you can safely include { and } in comments",
          "SELECT * FROM TABLE1 LEFT OUTER JOIN TABLE2 ON DEPT_NO = 003420930"
        };
    for (int i = 0; i < inputs.length; i++) {
      assertEquals(outputs[i], sharedConn.nativeSQL(inputs[i]));
    }
    assertEquals(
        "INSERT INTO TEST_SYNTAX_ERROR(str_value, json_value) VALUES ('abc\\\\', '{\"data\": \"test\"}')",
        sharedConn.nativeSQL(
            "INSERT INTO TEST_SYNTAX_ERROR(str_value, json_value) VALUES ('abc\\\\', '{\"data\": \"test\"}')"));

    try {
      sharedConn.nativeSQL("{call foo({fn now())}");
      fail("most have thrown an error");
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains("Invalid escape sequence , missing closing '}' character in '"));
    }

    try {
      sharedConn.nativeSQL("{call foo({unknown} )}");
      fail("most have thrown an error");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("unknown escape sequence {unknown}"));
    }
  }

  @Test
  public void nativeSQLNoBackSlash() throws SQLException {
    try (Connection con = createCon()) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      assertEquals("call foo('{' now())", con.nativeSQL("{call foo({fn '{' now()})}"));
    }
  }

  @Test
  public void nativeSqlTest() throws SQLException {
    // TODO: PLAT-5859
    // Extend with additional test cases if :> is implemented
    String exp =
        "SELECT convert(foo(a,b,c), SIGNED INTEGER)"
            + ", convert(convert(?, CHAR), SIGNED INTEGER)"
            + ", 1=?"
            + ", 1=?"
            + ", convert(?,   SIGNED INTEGER   )"
            + ",  convert (?,   SIGNED INTEGER   )"
            + ", convert(?, UNSIGNED INTEGER)"
            + ", convert(?, BINARY)"
            + ", convert(?, BINARY)"
            + ", convert(?, BINARY)"
            + ", convert(?, BINARY)"
            + ", convert(?, BINARY)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", convert(?, CHAR)"
            + ", 0.0+?"
            + ", 0.0+?"
            + ", convert(?, DECIMAL)"
            + ", convert(?, DECIMAL)"
            + ", convert(?, DECIMAL)"
            + ", convert(?, DATETIME)"
            + ", convert(?, DATETIME)";

    assertEquals(
        exp,
        sharedConn.nativeSQL(
            "SELECT {fn convert(foo(a,b,c), SQL_BIGINT)}"
                + ", {fn convert({fn convert(?, SQL_VARCHAR)}, SQL_BIGINT)}"
                + ", {fn convert(?, SQL_BOOLEAN )}"
                + ", {fn convert(?, BOOLEAN)}"
                + ", {fn convert(?,   SMALLINT   )}"
                + ", {fn  convert (?,   TINYINT   )}"
                + ", {fn convert(?, SQL_BIT)}"
                + ", {fn convert(?, SQL_BLOB)}"
                + ", {fn convert(?, SQL_VARBINARY)}"
                + ", {fn convert(?, SQL_LONGVARBINARY)}"
                + ", {fn convert(?, SQL_ROWID)}"
                + ", {fn convert(?, SQL_BINARY)}"
                + ", {fn convert(?, SQL_NCHAR)}"
                + ", {fn convert(?, SQL_CLOB)}"
                + ", {fn convert(?, SQL_NCLOB)}"
                + ", {fn convert(?, SQL_DATALINK)}"
                + ", {fn convert(?, SQL_VARCHAR)}"
                + ", {fn convert(?, SQL_NVARCHAR)}"
                + ", {fn convert(?, SQL_LONGVARCHAR)}"
                + ", {fn convert(?, SQL_LONGNVARCHAR)}"
                + ", {fn convert(?, SQL_SQLXML)}"
                + ", {fn convert(?, SQL_LONGNCHAR)}"
                + ", {fn convert(?, SQL_CHAR)}"
                + ", {fn convert(?, SQL_FLOAT)}"
                + ", {fn convert(?, SQL_DOUBLE)}"
                + ", {fn convert(?, SQL_DECIMAL)}"
                + ", {fn convert(?, SQL_REAL)}"
                + ", {fn convert(?, SQL_NUMERIC)}"
                + ", {fn convert(?, SQL_TIMESTAMP)}"
                + ", {fn convert(?, SQL_DATETIME)}"));
  }

  @Test
  public void doubleBackslash() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.setEscapeProcessing(true);
    stmt.execute("DROP TABLE IF EXISTS TEST_SYNTAX_ERROR");
    stmt.execute(
        "CREATE TABLE TEST_SYNTAX_ERROR("
            + "     id INTEGER unsigned NOT NULL AUTO_INCREMENT, "
            + "     str_value MEDIUMTEXT CHARACTER SET utf8mb4 NOT NULL,"
            + "     json_value  MEDIUMTEXT CHARACTER SET utf8mb4 NOT NULL, "
            + "    PRIMARY KEY ( id ))");
    stmt.execute(
        "INSERT INTO TEST_SYNTAX_ERROR(str_value, json_value) VALUES ('abc\\\\', '{\"data\": \"test\"}')");
  }

  @Test
  public void databaseStateChange() throws SQLException {
    try (Connection connection = createCon()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("drop database if exists _test_db");
        stmt.execute("create database _test_db");
        ResultSet rs = stmt.executeQuery("SELECT DATABASE()");
        rs.next();
        assertEquals(rs.getString(1), connection.getCatalog());
        stmt.execute("USE _test_db");
        assertEquals("_test_db", connection.getCatalog());
        stmt.execute("drop database _test_db");
      }
    }
  }

  @Test
  public void catalog() throws SQLException {
    try (Connection connection = createCon()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("drop database if exists _test_db");
        stmt.execute("create database _test_db");
        ResultSet rs = stmt.executeQuery("SELECT DATABASE()");
        rs.next();
        String initialCatalog = connection.getCatalog();
        assertEquals(rs.getString(1), initialCatalog);
        connection.setCatalog(initialCatalog);
        assertEquals(initialCatalog, connection.getCatalog());
        connection.setCatalog("_test_db");
        assertEquals("_test_db", connection.getCatalog());
        stmt.execute("USE _test_db");
        assertEquals("_test_db", connection.getCatalog());
        stmt.execute("drop database _test_db");
      }
    }
  }

  @Test
  public void checkFixedData() throws SQLException {
    sharedConn.unwrap(java.sql.Connection.class);
    assertThrowsContains(
        SQLException.class,
        () -> sharedConn.unwrap(String.class),
        "The receiver is not a wrapper for java.lang.String");
    assertTrue(sharedConn.createBlob() instanceof Blob);
    assertTrue(sharedConn.createClob() instanceof Clob);
    assertTrue(sharedConn.createNClob() instanceof NClob);
    assertThrows(SQLException.class, () -> sharedConn.createSQLXML());
    assertThrows(SQLException.class, () -> sharedConn.createArrayOf("", null));
    assertThrows(SQLException.class, () -> sharedConn.createStruct("", null));
    assertNull(sharedConn.getSchema());
    sharedConn.setSchema("fff");
    assertNull(sharedConn.getSchema());
  }

  @Test
  public void clientInfo() throws SQLException {
    assertTrue(sharedConn.getClientInfo().isEmpty());
    sharedConn.setClientInfo("some", "value");
    Properties props = new Properties();
    props.put("another", "one");
    props.put("and another", "two");
    sharedConn.setClientInfo(props);
    assertEquals(3, sharedConn.getClientInfo().size());
    assertEquals("value", sharedConn.getClientInfo("some"));
    assertNull(sharedConn.getClientInfo("some33"));
  }

  @Test
  public void abortTestAlreadyClosed() throws SQLException {
    Connection connection = createCon();
    connection.close();
    Executor executor = Runnable::run;
    connection.abort(executor);
  }

  @Test
  public void abortTestNoExecutor() {
    try {
      sharedConn.abort(null);
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot abort the connection: null executor passed"));
    }
  }

  @Test
  public void abortClose() throws Throwable {
    Connection connection = createCon();
    Statement stmt = connection.createStatement();
    stmt.setFetchSize(1);
    ResultSet rs =
        stmt.executeQuery(
            "select * from information_schema.columns as c1, "
                + "information_schema.tables, information_schema.tables as t2");
    assertTrue(rs.next());
    connection.abort(Runnable::run);
    // must still work

    Thread.sleep(20);
    try {
      assertTrue(rs.next());
      fail();
    } catch (SQLException sqle) {
      assertTrue(sqle.getMessage().contains("Operation not permit on a closed resultSet"));
    }
  }

  @Test
  public void verificationAbort() throws Throwable {
    Timer timer = new Timer();
    try (Connection connection = createCon()) {
      timer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              try {
                connection.abort(Runnable::run);
              } catch (SQLException sqle) {
                fail(sqle.getMessage());
              }
            }
          },
          10);

      Statement stmt = connection.createStatement();
      assertThrows(
          SQLException.class,
          () ->
              stmt.executeQuery(
                  "select * from information_schema.columns as c1,  information_schema.tables, information_schema.tables as t2"));
    }
  }

  @Test
  public void networkTimeoutTest() throws SQLException {
    try (Connection connection = createCon()) {
      assertEquals(0, connection.getNetworkTimeout());
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
      connection.setNetworkTimeout(executor, timeout);
      connection.isValid(2);
      assertEquals(timeout, connection.getNetworkTimeout());

      try {
        Statement stmt = connection.createStatement();
        stmt.execute("select sleep(2)");
        fail("Network timeout is " + timeout / 1000 + "sec, but slept for 2 sec");
      } catch (SQLException sqlex) {
        assertTrue(connection.isClosed());
      }
    }
  }

  @Test
  public void isolationLevel() throws SQLException {
    java.sql.Connection connection = createCon();
    int[] levels =
        new int[] {
          java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,
          java.sql.Connection.TRANSACTION_READ_COMMITTED,
          java.sql.Connection.TRANSACTION_SERIALIZABLE,
          java.sql.Connection.TRANSACTION_REPEATABLE_READ
        };
    for (int level : levels) {
      connection.setTransactionIsolation(level);
      assertEquals(level, connection.getTransactionIsolation());
    }
    connection.close();
    assertThrows(
        SQLException.class,
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));

    try (java.sql.Connection con2 = createCon()) {
      try {
        con2.setTransactionIsolation(10_000);
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Unsupported transaction isolation level"));
      }
    }
  }

  static class MySavepoint implements Savepoint {
    @Override
    public int getSavepointId() throws SQLException {
      return 0;
    }

    @Override
    public String getSavepointName() throws SQLException {
      return null;
    }
  }

  @Test
  public void savepointTest() throws SQLException {
    Connection con = createCon();
    try {
      con.setSavepoint();
    } catch (SQLFeatureNotSupportedException ignored) {
    }

    try {
      con.setSavepoint("test");
    } catch (SQLFeatureNotSupportedException ignored) {
    }

    try {
      MySavepoint savepoint = new MySavepoint();
      con.releaseSavepoint(savepoint);
    } catch (SQLFeatureNotSupportedException ignored) {
    }

    try {
      MySavepoint savepoint = new MySavepoint();
      con.rollback(savepoint);
    } catch (SQLFeatureNotSupportedException ignored) {
    }
  }

  @Test
  public void netWorkTimeout() throws SQLException {
    Connection con = createCon();
    assertThrowsContains(
        SQLException.class,
        () -> con.setNetworkTimeout(Runnable::run, -200),
        "Connection.setNetworkTimeout cannot be called with a negative timeout");
    con.close();
    assertThrowsContains(
        SQLException.class,
        () -> con.setNetworkTimeout(Runnable::run, 200),
        "Connection.setNetworkTimeout cannot be called on a closed connection");
  }

  @Nested
  @DisplayName("Transaction Test")
  class Transaction {

    @Test
    public void transactionTest() throws SQLException {
      Statement stmt = sharedConn.createStatement();
      try {
        stmt.execute(
            "CREATE TABLE transaction_test "
                + "(id int not null primary key auto_increment, test varchar(20))");
        sharedConn.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO transaction_test (test) VALUES ('heja')");
        stmt.executeUpdate("INSERT INTO transaction_test (test) VALUES ('japp')");
        sharedConn.commit();
        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_test ORDER BY id");
        assertEquals(true, rs.next());
        assertEquals("heja", rs.getString("test"));
        assertEquals(true, rs.next());
        assertEquals("japp", rs.getString("test"));
        assertEquals(false, rs.next());
        stmt.executeUpdate(
            "INSERT INTO transaction_test (test) VALUES ('rollmeback')",
            java.sql.Statement.RETURN_GENERATED_KEYS);
        ResultSet rsGen = stmt.getGeneratedKeys();
        rsGen.next();
        int[] autoInc = setAutoInc();
        assertEquals(autoInc[1] + autoInc[0] * 3, rsGen.getInt(1));
        sharedConn.rollback();
        rs = stmt.executeQuery("SELECT * FROM transaction_test WHERE id=3");
        assertEquals(false, rs.next());
        sharedConn.setAutoCommit(true);
      } finally {
        stmt.execute("DROP TABLE IF EXISTS transaction_test");
      }
    }

    /**
     * Get current autoincrement value, since Galera values are automatically set.
     *
     * @throws SQLException if any error occur.
     */
    public int[] setAutoInc() throws SQLException {
      return setAutoInc(1, 0);
    }

    /**
     * Get current autoincrement value, since Galera values are automatically set.
     *
     * @param autoIncInit default increment
     * @param autoIncOffsetInit default increment offset
     * @throws SQLException if any error occur
     * @see <a
     *     href="https://mariadb.org/auto-increments-in-galera/">https://mariadb.org/auto-increments-in-galera/</a>
     */
    public int[] setAutoInc(int autoIncInit, int autoIncOffsetInit) throws SQLException {

      int autoInc = autoIncInit;
      int autoIncOffset = autoIncOffsetInit;
      // in case of galera
      //      if (isGalera()) {
      //        ResultSet rs =
      //            sharedConn.createStatement().executeQuery("show variables like
      // '%auto_increment%'");
      //        while (rs.next()) {
      //          if ("auto_increment_increment".equals(rs.getString(1))) {
      //            autoInc = rs.getInt(2);
      //          }
      //          if ("auto_increment_offset".equals(rs.getString(1))) {
      //            autoIncOffset = rs.getInt(2);
      //          }
      //        }
      //        if (autoInc == 1) {
      //          // galera with one node only, then offset is not used
      //          autoIncOffset = 0;
      //        }
      //      }
      return new int[] {autoInc, autoIncOffset};
    }
  }

  @Test
  public void various() throws SQLException {
    assertThrows(SQLException.class, () -> sharedConn.setTypeMap(null));
    assertTrue(sharedConn.getTypeMap().isEmpty());
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, sharedConn.getHoldability());
    sharedConn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, sharedConn.getHoldability());
    assertNotEquals(0, sharedConn.getWaitTimeout());
  }

  @Test
  public void pamAuthPlugin() throws Throwable {

    Statement stmt = sharedConn.createStatement();

    stmt.execute("DROP USER IF EXISTS 'test_pam'");
    stmt.execute(
        "GRANT SELECT ON *.* TO 'test_pam' IDENTIFIED WITH "
            + "authentication_pam as 's2_pam_test'");

    try (Connection connection =
        createCon("user=test_pam&password=test_pass&restrictedAuth=mysql_clear_password")) {
      connection.getCatalog();
    }
    assertThrowsContains(
        SQLException.class,
        () -> createCon("user=test_pam&password=test_pass&restrictedAuth=other"),
        "Client restrict authentication plugin to a limited set of authentication");

    stmt.execute("drop user test_pam@'%'");
  }

  @Nested
  @DisplayName("Compression Test")
  class Compression {

    @Test
    public void testConnection() throws Exception {
      try (Connection connection = createCon("useCompression")) {
        // must have succeed
        connection.getCatalog();
      }
    }
  }

  @Test
  public void testNoUseReadAheadInputConnection() throws Exception {
    try (Connection connection = createCon("useReadAheadInput=false")) {
      // must have succeed
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.users");
      int i = 0;
      while (rs.next()) i++;
      assertTrue(i > 0);
    }
  }

  @Test
  public void useNoDatabase() throws SQLException {
    try (Connection con = createCon()) {
      String db = con.getCatalog();
      Statement stmt = con.createStatement();
      stmt.execute("CREATE DATABASE someDb");
      con.setCatalog("someDb");
      stmt.execute("DROP DATABASE someDb");
      if (minVersion(10, 4, 0)
          && !"maxscale".equals(System.getenv("srv"))
          && !"skysql-ha".equals(System.getenv("srv"))) {
        assertNull(con.getCatalog());
      }
    }
  }

  @Test
  public void windowsNamedPipe() throws SQLException {
    ResultSet rs = null;
    try {
      rs = sharedConn.createStatement().executeQuery("select @@named_pipe,@@socket");
    } catch (SQLException sqle) {
      // on non windows system, named_pipe doesn't exist.
    }
    if (rs != null) {
      assertTrue(rs.next());
      System.out.println("named_pipe:" + rs.getString(1));
      if (rs.getBoolean(1)) {
        String namedPipeName = rs.getString(2);
        System.out.println("namedPipeName:" + namedPipeName);

        // skip test if no namedPipeName was obtained because then we do not use a socket connection
        Assumptions.assumeTrue(namedPipeName != null);
        try (Connection connection = createCon("pipe=" + namedPipeName)) {
          java.sql.Statement stmt = connection.createStatement();
          try (ResultSet rs2 = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs2.next());
          }
        }
        // connection without host name
        try (java.sql.Connection connection =
            DriverManager.getConnection(
                "jdbc:singlestore:///"
                    + sharedConn.getCatalog()
                    + mDefUrl.substring(mDefUrl.indexOf("?user="))
                    + "&pipe="
                    + namedPipeName)) {
          java.sql.Statement stmt = connection.createStatement();
          try (ResultSet rs2 = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs2.next());
          }
        }
      }
    }
  }

  // TODO: PLAT-5887
  // we cannot really run this test since we run S2 in container
  @Test
  public void localSocket() throws Exception {
    Assumptions.assumeTrue(
        System.getenv("local") != null
            && "1".equals(System.getenv("local"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select @@version_compile_os,@@socket");
    if (!rs.next()) {
      return;
    }
    String path = rs.getString(2);
    stmt.execute("DROP USER IF EXISTS testSocket@'localhost'");
    stmt.execute("CREATE USER testSocket@'localhost' IDENTIFIED BY 'MySup5%rPassw@ord'");
    stmt.execute("GRANT SELECT on *.* to testSocket@'localhost' IDENTIFIED BY 'MySup5%rPassw@ord'");
    stmt.execute("FLUSH PRIVILEGES");

    try (java.sql.Connection connection =
        DriverManager.getConnection(
            "jdbc:singlestore:///"
                + sharedConn.getCatalog()
                + "?user=testSocket&password=MySup5%rPassw@ord&localSocket="
                + path)) {
      rs = connection.createStatement().executeQuery("select 1");
      assertTrue(rs.next());
    }

    assertThrowsContains(
        SQLException.class,
        () ->
            DriverManager.getConnection(
                "jdbc:singlestore:///"
                    + sharedConn.getCatalog()
                    + "?user=testSocket&password=MySup5%rPassw@ord&localSocket=/wrongPath"),
        "Socket fail to connect to host");

    if (haveSsl()) {
      String serverCertPath = SslTest.retrieveCertificatePath();
      if (serverCertPath != null) {
        try (Connection con =
            DriverManager.getConnection(
                "jdbc:singlestore:///"
                    + sharedConn.getCatalog()
                    + "?sslMode=verify-full&user=testSocket&password=MySup5%rPassw@ord"
                    + "&serverSslCert="
                    + serverCertPath
                    + "&localSocket="
                    + path)) {
          rs = con.createStatement().executeQuery("select 1");
          assertTrue(rs.next());
        }
      }
    }
    stmt.execute("DROP USER testSocket@'localhost'");
  }

  public boolean isLocalConnection(String testName) {
    boolean isLocal = false;
    try {
      if (InetAddress.getByName(hostname).isAnyLocalAddress()
          || InetAddress.getByName(hostname).isLoopbackAddress()) {
        isLocal = true;
      }
    } catch (UnknownHostException e) {
      // for some reason it wasn't possible to parse the hostname
      // do nothing
    }

    if (!isLocal) {
      System.out.println("test '" + testName + "' skipped because connection is not local");
    }

    return isLocal;
  }

  @Test
  public void socketFactoryTest() throws SQLException {
    try (Connection conn = createCon("socketFactory=" + SocketFactoryTest.class.getName())) {
      conn.isValid(1);
    }
    assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () -> createCon("socketFactory=wrongClass"),
        "Socket factory failed to initialized with option \"socketFactory\" set to \"wrongClass\"");
  }

  @Test
  public void socketOption() throws SQLException {
    try (Connection con = createCon("tcpKeepAlive=true&tcpAbortiveClose=true")) {
      con.isValid(1);
    }
  }

  @Test
  public void localSocketAddress() throws SQLException {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try (Connection con = createCon("localSocketAddress=" + hostAddress.host)) {
      con.isValid(1);
    }
  }

  @Test
  public void setReadOnly() throws SQLException {
    Connection con = createCon();
    con.setReadOnly(true);
    con.close();
    assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () -> con.setReadOnly(false),
        "Connection is closed");
  }

  @Test
  public void connectionCapabilities() {
    com.singlestore.jdbc.Connection con = sharedConn;
    long capabilities = con.getContext().getServerCapabilities();
    assertTrue((capabilities & Capabilities.CLIENT_MYSQL) > 0);
    assertTrue((capabilities & Capabilities.FOUND_ROWS) > 0);
    assertTrue((capabilities & Capabilities.LONG_FLAG) > 0);
    assertTrue((capabilities & Capabilities.CONNECT_WITH_DB) > 0);
    assertTrue((capabilities & Capabilities.NO_SCHEMA) > 0);
    assertTrue((capabilities & Capabilities.ODBC) > 0);
    assertTrue((capabilities & Capabilities.LOCAL_FILES) > 0);
    assertTrue((capabilities & Capabilities.IGNORE_SPACE) > 0);
    assertTrue((capabilities & Capabilities.CLIENT_PROTOCOL_41) > 0);
    assertTrue((capabilities & Capabilities.CLIENT_INTERACTIVE) > 0);
    assertTrue((capabilities & Capabilities.SSL) > 0);
    assertTrue((capabilities & Capabilities.IGNORE_SIGPIPE) > 0);
    assertTrue((capabilities & Capabilities.TRANSACTIONS) > 0);
    assertTrue((capabilities & Capabilities.RESERVED) > 0);
    assertTrue((capabilities & Capabilities.SECURE_CONNECTION) > 0);
    assertTrue((capabilities & Capabilities.MULTI_STATEMENTS) > 0);
    assertTrue((capabilities & Capabilities.MULTI_RESULTS) > 0);
    assertTrue((capabilities & Capabilities.PS_MULTI_RESULTS) > 0);
    assertTrue((capabilities & Capabilities.PLUGIN_AUTH) > 0);

    assertEquals(0, (capabilities & Capabilities.COMPRESS));
    assertEquals(0, (capabilities & Capabilities.CONNECT_ATTRS));
    assertEquals(0, (capabilities & Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA));
    assertEquals(0, (capabilities & Capabilities.CLIENT_SESSION_TRACK));
    assertEquals(0, (capabilities & Capabilities.CLIENT_DEPRECATE_EOF));
    assertEquals(0, (capabilities & Capabilities.COMPRESS));

    assertEquals(0, (capabilities & Capabilities.MARIADB_CLIENT_PROGRESS));
    assertEquals(0, (capabilities & Capabilities.MARIADB_CLIENT_COM_MULTI));
    assertEquals(0, (capabilities & Capabilities.MARIADB_CLIENT_STMT_BULK_OPERATIONS));
    assertEquals(0, (capabilities & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO));
    assertEquals(0, (capabilities & Capabilities.MARIADB_CLIENT_CACHE_METADATA));
  }
}
