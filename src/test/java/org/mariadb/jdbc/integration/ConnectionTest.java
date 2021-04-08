/*
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
 */

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

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
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.integration.util.SocketFactoryTest;

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
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

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
          "{call foo({fn now()})}",
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
          "call foo(now())",
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
      assertEquals(sharedConn.nativeSQL(inputs[i]), outputs[i]);
    }

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
  public void nativeSqlTest() throws SQLException {
    String exp;
    if (isMariaDBServer() || minVersion(8, 0, 17)) {
      exp =
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
              + ", convert(?, DOUBLE)"
              + ", convert(?, DOUBLE)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DATETIME)"
              + ", convert(?, DATETIME)";
    } else {
      exp =
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
    }

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
    Assumptions.assumeTrue(
        (isMariaDBServer() && minVersion(10, 2, 0)) || (!isMariaDBServer() && minVersion(5, 7, 0)));
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
    Assumptions.assumeTrue(
        (isMariaDBServer() && minVersion(10, 2, 0)) || (!isMariaDBServer() && minVersion(5, 7, 0)));
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
  public void testConnectionAttributes() throws Exception {
    // check if performance_schema is ON
    Statement stmt = sharedConn.createStatement();
    ResultSet res = stmt.executeQuery("show variables like 'performance_schema'");
    res.next();
    Assumptions.assumeFalse(res.getString("Value").equals("OFF"));

    try (Connection connection = createCon()) {
      Statement attributeStatement = connection.createStatement();
      ResultSet result =
          attributeStatement.executeQuery(
              "select * from performance_schema.session_connect_attrs where ATTR_NAME='_server_host' and processlist_id = connection_id()");
      while (result.next()) {
        String strVal = result.getString("ATTR_VALUE");
        assertEquals(Configuration.parse(mDefUrl).addresses().get(0).host, strVal);
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

  @Test
  public void savepointTest() throws SQLException {
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE spt(test varchar(10))");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO spt values('hej1')");
      stmt.executeUpdate("INSERT INTO spt values('hej2')");
      Savepoint savepoint = con.setSavepoint("ye`\\\\`p");
      stmt.executeUpdate("INSERT INTO spt  values('hej3')");
      stmt.executeUpdate("INSERT INTO spt values('hej4')");
      assertEquals("ye``\\\\``p", savepoint.getSavepointName());
      assertThrowsContains(
          SQLException.class,
          () -> savepoint.getSavepointId(),
          "Cannot retrieve savepoint id of a named savepoint");
      con.rollback(savepoint);
      stmt.executeUpdate("INSERT INTO spt values('hej5')");
      stmt.executeUpdate("INSERT INTO spt values('hej6')");
      con.commit();
      ResultSet rs = stmt.executeQuery("SELECT * FROM spt");
      assertTrue(rs.next());
      assertEquals("hej1", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("hej2", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("hej5", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("hej6", rs.getString(1));
      assertFalse(rs.next());
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

  @Test
  public void savepointUnname() throws SQLException {
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE spt(test varchar(10))");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO spt values('hej1')");
      stmt.executeUpdate("INSERT INTO spt values('hej2')");
      Savepoint savepoint = con.setSavepoint();
      stmt.executeUpdate("INSERT INTO spt  values('hej3')");
      stmt.executeUpdate("INSERT INTO spt values('hej4')");
      assertTrue(savepoint.getSavepointId() > 0);
      assertThrowsContains(
          SQLException.class,
          () -> savepoint.getSavepointName(),
          "Cannot retrieve savepoint name of an unnamed savepoint");
      con.rollback(savepoint);
      assertThrowsContains(
          SQLException.class, () -> con.rollback(new MySavepoint()), "Unknown savepoint type");
      stmt.executeUpdate("INSERT INTO spt values('hej5')");
      stmt.executeUpdate("INSERT INTO spt values('hej6')");
      con.commit();
      con.commit();
      ResultSet rs = stmt.executeQuery("SELECT * FROM spt");
      assertTrue(rs.next());
      assertEquals("hej1", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("hej2", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("hej5", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("hej6", rs.getString(1));
      assertFalse(rs.next());
    }
  }

  @Test
  public void releaseSavepoint() throws SQLException {
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE spt(test varchar(10))");
      con.setAutoCommit(false);
      stmt.executeUpdate("INSERT INTO spt values('hej1')");
      stmt.executeUpdate("INSERT INTO spt values('hej2')");
      Savepoint savepoint = con.setSavepoint();
      stmt.executeUpdate("INSERT INTO spt  values('hej3')");
      stmt.executeUpdate("INSERT INTO spt values('hej4')");
      con.releaseSavepoint(savepoint);
      assertThrowsContains(
          SQLException.class,
          () -> con.releaseSavepoint(new MySavepoint()),
          "Unknown savepoint type");
      stmt.executeUpdate("INSERT INTO spt values('hej5')");
      stmt.executeUpdate("INSERT INTO spt values('hej6')");
      con.commit();
      ResultSet rs = stmt.executeQuery("SELECT * FROM spt");
      for (int i = 1; i < 7; i++) {
        assertTrue(rs.next());
        assertEquals("hej" + i, rs.getString(1));
      }
      assertFalse(rs.next());
    }
  }

  class MySavepoint implements Savepoint {
    @Override
    public int getSavepointId() throws SQLException {
      return 0;
    }

    @Override
    public String getSavepointName() throws SQLException {
      return null;
    }
  }

  @Nested
  @DisplayName("Transaction Test")
  class Transaction {

    @Test
    public void testProperRollback() throws Exception {
      java.sql.Statement stmt = sharedConn.createStatement();
      try {
        stmt.execute("CREATE TABLE tx_prim_key(id int not null primary key) engine=innodb");
        stmt.execute(
            "CREATE TABLE tx_fore_key (id int not null primary key, id_ref int not null, "
                + "foreign key (id_ref) references tx_prim_key(id) on delete restrict on update restrict) "
                + "engine=innodb");
        stmt.executeUpdate("insert into tx_prim_key(id) values(32)");
        stmt.executeUpdate("insert into tx_fore_key(id, id_ref) values(42, 32)");

        // 2. try to delete entry in Primary table in a transaction - which will fail due
        // foreign key.
        sharedConn.setAutoCommit(false);
        try (java.sql.Statement st = sharedConn.createStatement()) {
          st.executeUpdate("delete from tx_prim_key where id = 32");
          sharedConn.commit();
          fail("Expected SQLException");
        } catch (SQLException e) {
          // This exception is expected
          assertTrue(e.getMessage().contains("a foreign key constraint fails"));
          sharedConn.rollback();
        }

        try (java.sql.Connection conn2 = createCon();
            java.sql.Statement st = conn2.createStatement()) {
          st.setQueryTimeout(30000);
          st.executeUpdate("delete from tx_fore_key where id = 42");
          st.executeUpdate("delete from tx_prim_key where id = 32");
        }

      } finally {
        stmt.execute("drop table if exists tx_fore_key");
        stmt.execute("drop table if exists tx_prim_key");
      }
    }

    @Test
    public void transactionTest() throws SQLException {
      Statement stmt = sharedConn.createStatement();
      try {
        stmt.execute(
            "CREATE TABLE transaction_test "
                + "(id int not null primary key auto_increment, test varchar(20)) "
                + "engine=innodb");
        sharedConn.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO transaction_test (test) VALUES ('heja')");
        stmt.executeUpdate("INSERT INTO transaction_test (test) VALUES ('japp')");
        sharedConn.commit();
        ResultSet rs = stmt.executeQuery("SELECT * FROM transaction_test");
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
  public void verificationEd25519AuthPlugin() throws Throwable {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && isMariaDBServer()
            && minVersion(10, 2, 0));
    Statement stmt = sharedConn.createStatement();

    try {
      stmt.execute("INSTALL SONAME 'auth_ed25519'");
    } catch (SQLException sqle) {
      Assumptions.assumeTrue(false, "server doesn't have ed25519 plugin, cancelling test");
    }
    stmt.execute("drop user if exists verificationEd25519AuthPlugin@'%'");
    try {
      if (minVersion(10, 4, 0)) {
        stmt.execute(
            "CREATE USER IF NOT EXISTS verificationEd25519AuthPlugin@'%' IDENTIFIED "
                + "VIA ed25519 USING PASSWORD('MySup8%rPassw@ord')");
      } else {
        stmt.execute(
            "CREATE USER IF NOT EXISTS verificationEd25519AuthPlugin@'%' IDENTIFIED "
                + "VIA ed25519 USING '6aW9C7ENlasUfymtfMvMZZtnkCVlcb1ssxOLJ0kj/AA'");
      }
    } catch (SQLException sqle) {
      // already existing
    }
    stmt.execute(
        "GRANT SELECT on " + sharedConn.getCatalog() + ".* to verificationEd25519AuthPlugin");

    try (Connection connection =
        createCon("user=verificationEd25519AuthPlugin&password=MySup8%rPassw@ord")) {
      // must have succeed
      connection.getCatalog();
    }
    stmt.execute("drop user verificationEd25519AuthPlugin@'%'");
  }

  @Test
  public void pamAuthPlugin() throws Throwable {
    // https://mariadb.com/kb/en/authentication-plugin-pam/
    // only test on travis, because only work on Unix-like operating systems.
    // /etc/pam.d/mariadb pam configuration is created beforehand

    Assumptions.assumeTrue(
        isMariaDBServer()
            && System.getenv("TEST_PAM_USER") != null
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("INSTALL PLUGIN pam SONAME 'auth_pam'");
    } catch (SQLException sqle) {
      // might be already set
    }
    stmt.execute("DROP USER IF EXISTS 'testPam'@'%'");
    stmt.execute("CREATE USER 'testPam'@'%' IDENTIFIED VIA pam USING 'mariadb'");
    stmt.execute("GRANT SELECT ON *.* TO 'testPam'@'%' IDENTIFIED VIA pam");
    stmt.execute("FLUSH PRIVILEGES");

    try (Connection connection = createCon("user=testPam&password=myPwd")) {
      // must have succeed
      connection.getCatalog();
    }
    stmt.execute("drop user testPam@'%'");
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
    Assumptions.assumeTrue(
        isMariaDBServer()
            && System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    try (ResultSet rs = sharedConn.createStatement().executeQuery("select @@named_pipe,@@socket")) {
      assertTrue(rs.next());
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
                "jdbc:mariadb:///"
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
            "jdbc:mariadb:///"
                + sharedConn.getCatalog()
                + "?user=testSocket&password=MySup5%rPassw@ord&localSocket="
                + path)) {
      rs = connection.createStatement().executeQuery("select 1");
      assertTrue(rs.next());
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
  public void sslNotSet() throws SQLException {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeFalse(haveSsl());
    assertThrowsContains(
        SQLException.class, () -> createCon("sslMode=trust"), "ssl not enabled in the server");
  }

  @Test
  public void localSocketAddress() throws SQLException {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try (Connection con = createCon("localSocketAddress=" + hostAddress.host)) {
      con.isValid(1);
    }
  }
}
