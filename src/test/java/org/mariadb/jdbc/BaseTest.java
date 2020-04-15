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

import com.sun.jna.Platform;
import java.io.*;
import java.lang.Thread.State;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mariadb.jdbc.failover.TcpProxy;
import org.mariadb.jdbc.internal.failover.AbstractMastersListener;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.util.Options;

/**
 * Base util class. For testing mvn test -DdbUrl=jdbc:mariadb://localhost:3306/testj?user=root
 * -DlogLevel=FINEST
 */
@Ignore
@SuppressWarnings("Annotator")
public class BaseTest {
  protected static String mDefUrl;

  static {
    try (InputStream inputStream =
        BaseTest.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);
      mDefUrl =
          String.format(
              "jdbc:mariadb://%s:%s/%s?user=%s&password=%s&%s",
              prop.getProperty("DB_HOST"),
              prop.getProperty("DB_PORT"),
              prop.getProperty("DB_DATABASE"),
              prop.getProperty("DB_USER"),
              prop.getProperty("DB_PASSWORD"),
              prop.getProperty("DB_OTHER"));

    } catch (IOException io) {
      io.printStackTrace();
    }
  }

  private static final Set<String> tempTableList = new HashSet<>();
  private static final Set<String> tempViewList = new HashSet<>();
  private static final Set<String> tempProcedureList = new HashSet<>();
  private static final Set<String> tempFunctionList = new HashSet<>();
  private static final NumberFormat numberFormat = DecimalFormat.getInstance(Locale.ROOT);
  protected static String connU;
  protected static String connUri;
  protected static String connDnsUri;
  protected static String hostname;
  protected static int port;
  protected static String database;
  protected static String username;
  protected static String password;
  protected static Options options;
  protected static String parameters;
  protected static boolean testSingleHost;
  protected static Connection sharedConnection;
  protected static boolean runLongTest = false;
  protected static boolean doPrecisionTest = true;
  private static TcpProxy proxy = null;
  private static UrlParser urlParser;

  @Rule
  public TestRule watcher =
      new TestWatcher() {
        private long ttime;

        protected void starting(Description description) {
          if (testSingleHost) {
            System.out.println(
                "start test : " + description.getClassName() + "." + description.getMethodName());
            ttime = System.nanoTime();
          }
        }

        // execute another query to ensure connection is stable
        protected void finished(Description description) {
          if (testSingleHost) {
            Random random = new Random();
            int randInt = random.nextInt();
            try (PreparedStatement preparedStatement =
                sharedConnection.prepareStatement("SELECT " + randInt)) {
              ResultSet rs = preparedStatement.executeQuery();
              assertTrue(rs.next());
              assertEquals(randInt, rs.getInt(1));
            } catch (SQLNonTransientConnectionException connFail) {
              connFail.printStackTrace();
              try {
                beforeClassBaseTest();
              } catch (SQLException e) {
                System.out.println("ERROR reconnecting");
                e.printStackTrace();
              }
              fail(
                  "Prepare after test fail for "
                      + description.getClassName()
                      + "."
                      + description.getMethodName());

            } catch (Exception e) {
              e.printStackTrace();
              fail(
                  "Prepare after test fail for "
                      + description.getClassName()
                      + "."
                      + description.getMethodName());
            }
          }
        }

        protected void succeeded(Description description) {
          if (testSingleHost) {
            System.out.println(
                "finished test success : "
                    + description.getClassName()
                    + "."
                    + description.getMethodName()
                    + " after "
                    + numberFormat.format(((double) System.nanoTime() - ttime) / 1000000)
                    + " ms");
          }
        }

        protected void failed(Throwable throwable, Description description) {
          if (testSingleHost) {
            System.out.println(
                "finished test failed : "
                    + description.getClassName()
                    + "."
                    + description.getMethodName()
                    + " after "
                    + numberFormat.format(((double) System.nanoTime() - ttime) / 1000000)
                    + " ms");
          }
        }
      };

  /**
   * Initialization.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void beforeClassBaseTest() throws SQLException {
    String url = System.getProperty("dbUrl", mDefUrl);
    runLongTest = Boolean.parseBoolean(System.getProperty("runLongTest", "false"));
    testSingleHost = Boolean.parseBoolean(System.getProperty("testSingleHost", "true"));

    if (testSingleHost) {
      urlParser = UrlParser.parse(url); // + "&pool=true&maxPoolSize=2&minPoolSize=1");
      if (urlParser.getHostAddresses().size() > 0) {
        hostname = urlParser.getHostAddresses().get(0).host;
        port = urlParser.getHostAddresses().get(0).port;
      } else {
        hostname = null;
        port = 3306;
      }
      database = urlParser.getDatabase();
      username = urlParser.getUsername();
      password = urlParser.getPassword();
      options = urlParser.getOptions();
      int separator = url.indexOf("//");
      String urlSecondPart = url.substring(separator + 2);
      int dbIndex = urlSecondPart.indexOf("/");
      int paramIndex = urlSecondPart.indexOf("?");

      String additionalParameters;
      if ((dbIndex < paramIndex && dbIndex < 0) || (dbIndex > paramIndex && paramIndex > -1)) {
        additionalParameters = urlSecondPart.substring(paramIndex);
      } else if ((dbIndex < paramIndex && dbIndex > -1)
          || (dbIndex > paramIndex && paramIndex < 0)) {
        additionalParameters = urlSecondPart.substring(dbIndex);
      } else {
        additionalParameters = null;
      }
      if (additionalParameters != null) {
        parameters = "";
        if (additionalParameters.indexOf("?") >= 0) {
          parameters = additionalParameters.substring(additionalParameters.indexOf("?") + 1);
        }
      } else {
        parameters = null;
      }

      setUri();
      urlParser.auroraPipelineQuirks();

      try {
        sharedConnection = MariaDbConnection.newConnection(urlParser, null);
      } catch (SQLException sqle) {
        System.out.println("Connection from pool fail :" + sqle.getMessage());
        sharedConnection = DriverManager.getConnection(url);
      }

      String dbVersion = sharedConnection.getMetaData().getDatabaseProductVersion();
      doPrecisionTest =
          isMariadbServer() || !dbVersion.startsWith("5.5"); // MySQL 5.5 doesn't support precision
    }
  }

  private static void setUri() {
    connU =
        "jdbc:mariadb://"
            + ((hostname == null) ? "localhost" : hostname)
            + ":"
            + port
            + "/"
            + ((database == null) ? "" : database);
    connUri = connU + "?" + parameters;
    connDnsUri =
        "jdbc:mariadb://mariadb.example.com:"
            + port
            + "/"
            + database
            + "?"
            + parameters
            + (password != null && !"".equals(password) ? "&password=" + password : "");
  }

  /**
   * Destroy the test tables.
   *
   * @throws SQLException exception
   */
  @AfterClass
  public static void afterClassBaseTest() throws SQLException {
    if (testSingleHost && sharedConnection != null && !sharedConnection.isClosed()) {
      if (!tempViewList.isEmpty()) {
        Statement stmt = sharedConnection.createStatement();
        for (String viewName : tempViewList) {
          try {
            stmt.execute("DROP VIEW IF EXISTS " + viewName);
          } catch (SQLException e) {
            // eat exception
          }
        }
      }
      if (!tempTableList.isEmpty()) {
        Statement stmt = sharedConnection.createStatement();
        for (String tableName : tempTableList) {
          try {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
          } catch (SQLException e) {
            // eat exception
          }
        }
      }
      if (!tempProcedureList.isEmpty()) {
        Statement stmt = sharedConnection.createStatement();
        for (String procedureName : tempProcedureList) {
          try {
            stmt.execute("DROP procedure IF EXISTS " + procedureName);
          } catch (SQLException e) {
            // eat exception
          }
        }
      }
      if (!tempFunctionList.isEmpty()) {
        Statement stmt = sharedConnection.createStatement();
        for (String functionName : tempFunctionList) {
          try {
            stmt.execute("DROP FUNCTION IF EXISTS " + functionName);
          } catch (SQLException e) {
            // eat exception
          }
        }
      }

      try {
        sharedConnection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    if (!Platform.isWindows()) {
      Iterator<Thread> it = Thread.getAllStackTraces().keySet().iterator();
      Thread thread;
      while (it.hasNext()) {
        thread = it.next();
        if (thread.getName().contains("MariaDb-bulk-")) {
          if (thread.getState() != State.WAITING) {
            // print stack trace to console.
            for (StackTraceElement ste : thread.getStackTrace()) {
              System.out.println(ste);
            }
          }
          assertEquals(State.WAITING, thread.getState());
        }
      }
    }
  }

  // common function for logging information
  static void logInfo(String message) {
    System.out.println(message);
  }

  /**
   * Create a table that will be detroyed a the end of tests.
   *
   * @param tableName table name
   * @param tableColumns table columns
   * @throws SQLException exception
   */
  public static void createTable(String tableName, String tableColumns) throws SQLException {
    createTable(tableName, tableColumns, null);
  }

  /**
   * Create a table that will be detroyed a the end of tests.
   *
   * @param tableName table name
   * @param tableColumns table columns
   * @param engine engine type
   * @throws SQLException exception
   */
  public static void createTable(String tableName, String tableColumns, String engine)
      throws SQLException {
    if (testSingleHost) {
      Statement stmt = sharedConnection.createStatement();
      stmt.execute("drop table if exists " + tableName);
      stmt.execute(
          "create table "
              + tableName
              + " ("
              + tableColumns
              + ") "
              + ((engine != null) ? engine : ""));
      if (!tempFunctionList.contains(tableName)) {
        tempTableList.add(tableName);
      }
    }
  }

  /**
   * Create a view that will be detroyed a the end of tests.
   *
   * @param viewName table name
   * @param tableColumns table columns
   * @throws SQLException exception
   */
  public static void createView(String viewName, String tableColumns) throws SQLException {
    if (testSingleHost) {
      Statement stmt = sharedConnection.createStatement();
      stmt.execute("drop view if exists " + viewName);
      stmt.execute("create view " + viewName + " AS (" + tableColumns + ") ");
      tempViewList.add(viewName);
    }
  }

  /**
   * Create procedure that will be delete on end of test.
   *
   * @param name procedure name
   * @param body procecure body
   * @throws SQLException exception
   */
  public static void createProcedure(String name, String body) throws SQLException {
    if (testSingleHost) {
      Statement stmt = sharedConnection.createStatement();
      stmt.execute("drop procedure IF EXISTS " + name);
      stmt.execute("create  procedure " + name + body);
      tempProcedureList.add(name);
    }
  }

  /**
   * Create function that will be delete on end of test.
   *
   * @param name function name
   * @param body function body
   * @throws SQLException exception
   */
  public static void createFunction(String name, String body) throws SQLException {
    if (testSingleHost) {
      Statement stmt = sharedConnection.createStatement();
      stmt.execute("drop function IF EXISTS " + name);
      stmt.execute("create function " + name + body);
      tempFunctionList.add(name);
    }
  }

  /**
   * Check if current DB server is MariaDB.
   *
   * @return true if DB is mariadb
   * @throws SQLException exception
   */
  static boolean isMariadbServer() throws SQLException {
    if (testSingleHost) {
      DatabaseMetaData md = sharedConnection.getMetaData();
      return md.getDatabaseProductVersion().contains("MariaDB");
    }
    return false;
  }

  /**
   * List current connections to server.
   *
   * @return number of thread connected.
   * @throws SQLException if queries failed
   */
  public static int getCurrentConnections() {
    try {
      Statement stmt = sharedConnection.createStatement();
      ResultSet rs = stmt.executeQuery("show status where `variable_name` = 'Threads_connected'");
      assertTrue(rs.next());
      return rs.getInt(2);

    } catch (SQLException e) {
      return -1;
    }
  }

  /**
   * Check if version if at minimum the version asked.
   *
   * @param major database major version
   * @param minor database minor version
   * @param patch database patch version
   * @throws SQLException exception
   */
  public static boolean minVersion(int major, int minor, int patch) {
    return ((MariaDbConnection) sharedConnection).versionGreaterOrEqual(major, minor, patch);
  }

  /**
   * Indicate if there is a anonymous user.
   *
   * @return true if anonymous user exist
   * @throws SQLException if any error occur
   */
  public boolean anonymousUser() throws SQLException {
    if (testSingleHost) {
      Statement stmt = sharedConnection.createStatement();
      ResultSet rs =
          stmt.executeQuery("SELECT * FROM mysql.user u where u.Host='localhost' and u.User=''");
      return rs.next();
    }
    return false;
  }

  /**
   * Create a connection with proxy.
   *
   * @param info additionnal properties
   * @return a proxyfied connection
   * @throws SQLException if any error occur
   */
  public Connection createProxyConnection(Properties info) throws SQLException {
    UrlParser tmpUrlParser = UrlParser.parse(connUri);
    username = tmpUrlParser.getUsername();
    hostname = tmpUrlParser.getHostAddresses().get(0).host;
    String sockethosts = "";
    HostAddress hostAddress;
    try {
      hostAddress = tmpUrlParser.getHostAddresses().get(0);
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
      sockethosts +=
          "address=(host=localhost)(port="
              + proxy.getLocalPort()
              + ")"
              + ((hostAddress.type != null) ? "(type=" + hostAddress.type + ")" : "");
    } catch (IOException e) {
      e.printStackTrace();
    }
    String[] splitValue = connUri.split("/");
    String[] subarray = Arrays.asList(splitValue)
            .subList(3, splitValue.length)
            .toArray(new String[0]);
    String dbAndParameters =  String.join("/", subarray);

    return openConnection(
        "jdbc:mariadb://"
            + sockethosts
            + "/"
            + dbAndParameters
            + ((options.useSsl != null)
                ? "&useSsl" + options.useSsl + "&disableSslHostnameVerification"
                : "")
            + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : ""),
        info);
  }

  /**
   * Stop proxy, and restart it after a certain amount of time.
   *
   * @param millissecond milliseconds
   */
  public void stopProxy(long millissecond) {
    proxy.restart(millissecond);
  }

  /** Stop proxy. */
  public void stopProxy() {
    proxy.stop();
  }

  public void delayProxy(int millissecond) {
    proxy.setDelay(millissecond);
  }

  public void forceCloseProxy() {
    proxy.forceClose();
  }

  public void removeDelayProxy() {
    proxy.removeDelay();
  }

  /** Restart proxy. */
  public void restartProxy() {
    proxy.restart();
  }

  /** Clean proxies. */
  public void closeProxy() {
    try {
      proxy.stop();
      proxy = null;
    } catch (Exception e) {
      // Eat exception
    }
  }

  @Before
  public void init() {
    Assume.assumeTrue(testSingleHost);
  }

  /**
   * Permit to assure that host are not in a blacklist after a test.
   *
   * @param connection connection
   */
  public void assureBlackList(Connection connection) {
    AbstractMastersListener.clearBlacklist();
  }

  protected Protocol getProtocolFromConnection(Connection conn) throws Throwable {

    Method getProtocol = MariaDbConnection.class.getDeclaredMethod("getProtocol");
    getProtocol.setAccessible(true);
    Object obj = getProtocol.invoke(conn);
    return (Protocol) obj;
  }

  protected void setHostname(String hostname) throws SQLException {
    BaseTest.hostname = hostname;
    setUri();
    setConnection();
  }

  protected void setPort(int port) throws SQLException {
    BaseTest.port = port;
    setUri();
    setConnection();
  }

  protected void setDatabase(String database) throws SQLException {
    BaseTest.database = database;
    BaseTest.setUri();
    setConnection();
  }

  protected void setUsername(String username) throws SQLException {
    BaseTest.username = username;
    setUri();
    setConnection();
  }

  protected void setPassword(String password) throws SQLException {
    BaseTest.password = password;
    setUri();
    setConnection();
  }

  protected Connection setBlankConnection(String parameters) throws SQLException {
    return openConnection(
        connU
            + "?user="
            + username
            + (password != null && !"".equals(password) ? "&password=" + password : "")
            + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
            + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
            + parameters,
        null);
  }

  protected Connection setConnection() throws SQLException {
    return openConnection(connUri, null);
  }

  protected Connection setConnection(Map<String, String> props) throws SQLException {
    Properties info = new Properties();
    for (String key : props.keySet()) {
      info.setProperty(key, props.get(key));
    }
    return openConnection(connU, info);
  }

  protected Connection setConnection(Properties info) throws SQLException {
    return openConnection(connUri, info);
  }

  protected Connection setConnection(String parameters) throws SQLException {
    return openConnection(connUri + parameters, null);
  }

  protected Connection setConnection(String additionalParameters, String database)
      throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:mariadb://");
    if (hostname == null) {
      sb.append("localhost");
    } else {
      sb.append(hostname);
    }
    sb.append(":").append(port).append("/");
    if (database != null) {
      sb.append(database);
    }
    sb.append("?user=").append(username);
    if (password != null && !password.isEmpty()) {
      sb.append("&password=").append(password);
    }
    if (parameters != null && !parameters.isEmpty()) {
      sb.append("&").append(parameters);
    }
    sb.append(additionalParameters);
    return openConnection(sb.toString(), null);
  }

  protected Connection setDnsConnection(String parameters) throws SQLException {
    String connU =
        "jdbc:mariadb://mariadb.example.com:" + port + "/" + ((database == null) ? "" : database);
    String connUri =
        connU
            + "?user="
            + username
            + (password != null && !"".equals(password) ? "&password=" + password : "")
            + (parameters != null ? "&" + parameters : "");
    return openConnection(connUri + parameters, null);
  }

  /**
   * Permit to reconstruct a connection.
   *
   * @param uri base uri
   * @param info additionnal properties
   * @return A connection
   * @throws SQLException is any error occur
   */
  public Connection openConnection(String uri, Properties info) throws SQLException {
    if (info == null) {
      return DriverManager.getConnection(uri);
    } else {
      return DriverManager.getConnection(uri, info);
    }
  }

  protected Connection openNewConnection(String url) throws SQLException {
    return DriverManager.getConnection(url);
  }

  protected Connection openNewConnection(String url, Properties info) throws SQLException {
    return DriverManager.getConnection(url, info);
  }

  protected boolean isGalera() {
    try {
      Statement st = sharedConnection.createStatement();
      ResultSet rs = st.executeQuery("show status like 'wsrep_cluster_size'");
      if (rs.next()) {
        return rs.getInt(2) > 0;
      }
    } catch (SQLException sqle) {
      // skip
    }
    return false;
  }

  /**
   * Check if max_allowed_packet value is equal or greater then 8m.
   *
   * @param testName test method name
   * @return true if max_allowed_packet value is equal or greater then 8m.
   * @throws SQLException if connection fail
   */
  public boolean checkMaxAllowedPacketMore8m(String testName) throws SQLException {
    Statement st = sharedConnection.createStatement();
    ResultSet rs = st.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    long maxAllowedPacket = rs.getLong(1);

    if (maxAllowedPacket < 8 * 1024 * 1024L) {

      System.out.println(
          "test '" + testName + "' skipped  due to server variable max_allowed_packet < 8M");
      return false;
    }

    return true;
  }

  /**
   * Check if max_allowed_packet value is equal or greater then 20m.
   *
   * @param testName test method name
   * @return true if max_allowed_packet value is equal or greater then 20m.
   * @throws SQLException if connection fail
   */
  public boolean checkMaxAllowedPacketMore20m(String testName) throws SQLException {
    return checkMaxAllowedPacketMore20m(testName, true);
  }

  /**
   * Check if max_allowed_packet value is equal or greater then 20m.
   *
   * @param testName test method name
   * @param displayMessage message to display in case of error.
   * @return true if max_allowed_packet value is equal or greater then 20m.
   * @throws SQLException if connection fail
   */
  public boolean checkMaxAllowedPacketMore20m(String testName, boolean displayMessage)
      throws SQLException {
    Statement st = sharedConnection.createStatement();
    ResultSet rs = st.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    long maxAllowedPacket = rs.getLong(1);

    if (maxAllowedPacket < 20 * 1024 * 1024L) {

      if (displayMessage) {
        System.out.println(
            "test '" + testName + "' skipped  due to server variable max_allowed_packet < 20M");
      }
      return false;
    }

    return true;
  }

  /**
   * Check if max_allowed_packet value is equal or greater then 40m.
   *
   * @param testName test method name
   * @return true if max_allowed_packet value is equal or greater then 40m.
   * @throws SQLException if connection fail
   */
  public boolean checkMaxAllowedPacketMore40m(String testName) throws SQLException {
    return checkMaxAllowedPacketMore40m(testName, true);
  }

  /**
   * Check if max_allowed_packet value is equal or greater then 40m.
   *
   * @param testName test method name
   * @param displayMsg message to display in case of error.
   * @return true if max_allowed_packet value is equal or greater then 40m.
   * @throws SQLException if connection fail
   */
  public boolean checkMaxAllowedPacketMore40m(String testName, boolean displayMsg)
      throws SQLException {
    Statement st = sharedConnection.createStatement();
    ResultSet rs = st.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    long maxAllowedPacket = rs.getLong(1);

    if (maxAllowedPacket < 40 * 1024 * 1024L) {
      if (displayMsg) {
        System.out.println(
            "test '" + testName + "' skipped  due to server variable max_allowed_packet < 40M");
      }
      return false;
    }

    return true;
  }

  /**
   * Does the user have super privileges.
   *
   * @param testName test name
   * @return true if super user
   * @throws SQLException in any connection occur
   */
  public boolean hasSuperPrivilege(String testName) throws SQLException {
    boolean superPrivilege = false;
    try (Statement st = sharedConnection.createStatement()) {
      // first test for specific user and host combination
      try (ResultSet rs =
          st.executeQuery(
              "SELECT Super_Priv FROM mysql.user WHERE user = '"
                  + username
                  + "' AND host = '"
                  + hostname
                  + "'")) {
        if (rs.next()) {
          superPrivilege = (rs.getString(1).equals("Y"));
        } else {
          // then check for user on whatever (%) host
          try (ResultSet rs2 =
              st.executeQuery(
                  "SELECT Super_Priv FROM mysql.user WHERE user = '"
                      + username
                      + "' AND host = '%'")) {
            if (rs2.next()) {
              superPrivilege = (rs2.getString(1).equals("Y"));
            }
          }
        }
      }
    }

    if (!superPrivilege) {
      System.out.println(
          "test '"
              + testName
              + "' skipped because user '"
              + username
              + "' doesn't have SUPER privileges");
    }

    return superPrivilege;
  }

  /**
   * Is the connection local.
   *
   * @param testName test method name
   * @return true if local
   */
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

  /**
   * Indicate if server has ssl configured.
   *
   * @param connection connection to check
   * @return true if SSL is enabled
   */
  public boolean haveSsl(Connection connection) {
    try {
      ResultSet rs = connection.createStatement().executeQuery("select @@have_ssl");
      assertTrue(rs.next());
      return "YES".equals(rs.getString(1));
    } catch (Exception e) {
      return false; /* maybe 4.x ? */
    }
  }

  /**
   * Check if version if at minimum the version asked.
   *
   * @param major database major version
   * @param minor database minor version
   * @throws SQLException exception
   */
  public boolean minVersion(int major, int minor) throws SQLException {
    DatabaseMetaData md = sharedConnection.getMetaData();
    int dbMajor = md.getDatabaseMajorVersion();
    int dbMinor = md.getDatabaseMinorVersion();
    return (dbMajor > major || (dbMajor == major && dbMinor >= minor));
  }

  /**
   * Check if version if before the version asked.
   *
   * @param major database major version
   * @param minor database minor version
   * @throws SQLException exception
   */
  public boolean strictBeforeVersion(int major, int minor) throws SQLException {
    DatabaseMetaData md = sharedConnection.getMetaData();
    int dbMajor = md.getDatabaseMajorVersion();
    int dbMinor = md.getDatabaseMinorVersion();
    return (dbMajor < major || (dbMajor == major && dbMinor < minor));
  }

  /**
   * Cancel if database version match.
   *
   * @param major db major version
   * @param minor db minor version
   * @throws SQLException exception
   */
  public void cancelForVersion(int major, int minor) throws SQLException {

    String dbVersion = sharedConnection.getMetaData().getDatabaseProductVersion();
    Assume.assumeFalse(dbVersion.startsWith(major + "." + minor));
  }

  /**
   * Cancel if database version match.
   *
   * @param major db major version
   * @param minor db minor version
   * @param patch db patch version
   * @throws SQLException exception
   */
  public void cancelForVersion(int major, int minor, int patch) throws SQLException {
    String dbVersion = sharedConnection.getMetaData().getDatabaseProductVersion();
    Assume.assumeFalse(dbVersion.startsWith(major + "." + minor + "." + patch));
  }

  public void requireMinimumVersion(int major, int minor) throws SQLException {
    Assume.assumeTrue(minVersion(major, minor));
  }

  /**
   * Cancel if Maxscale version isn't required minimum.
   *
   * @param major minimum maxscale major version
   * @param minor minimum maxscale minor version
   */
  public void ifMaxscaleRequireMinimumVersion(int major, int minor) {
    String maxscaleVersion = System.getenv("MAXSCALE_VERSION");
    if (maxscaleVersion == null) {
      return;
    }

    String[] versionArray = maxscaleVersion.split("[^0-9]");

    int majorVersion = 0;
    int minorVersion = 0;

    // standard version
    if (versionArray.length > 2) {

      majorVersion = Integer.parseInt(versionArray[0]);
      minorVersion = Integer.parseInt(versionArray[1]);

    } else {

      if (versionArray.length > 0) {
        majorVersion = Integer.parseInt(versionArray[0]);
      }

      if (versionArray.length > 1) {
        minorVersion = Integer.parseInt(versionArray[1]);
      }
    }

    Assume.assumeTrue(majorVersion > major || (majorVersion == major && minorVersion >= minor));
  }

  /**
   * Change session time zone.
   *
   * @param connection connection
   * @param timeZone timezone to set
   * @throws SQLException exception
   */
  public void setSessionTimeZone(Connection connection, String timeZone) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("set @@session.time_zone = '" + timeZone + "'");
    }
  }

  /**
   * Get row number.
   *
   * @param tableName table name
   * @return resultset number in this table
   * @throws SQLException if error occur
   */
  public int getRowCount(String tableName) throws SQLException {
    ResultSet rs =
        sharedConnection.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
    if (rs.next()) {
      return rs.getInt(1);
    }
    throw new SQLException("No table " + tableName + " found");
  }

  /**
   * Permit to know if sharedConnection will use Prepare. (in case dbUrl modify default options)
   *
   * @return true if PreparedStatement will use Prepare.
   */
  public boolean sharedUsePrepare() {
    return urlParser.getOptions().useServerPrepStmts
        && !urlParser.getOptions().rewriteBatchedStatements;
  }

  /**
   * Permit access to current sharedConnection options.
   *
   * @return Options
   */
  public Options sharedOptions() {
    return urlParser.getOptions();
  }

  /**
   * Permit to know if sharedConnection use rewriteBatchedStatements.
   *
   * @return true if option rewriteBatchedStatements is set to true
   */
  public boolean sharedIsRewrite() {
    return urlParser.getOptions().rewriteBatchedStatements;
  }

  /**
   * Has server bulk capacity.
   *
   * @return true if server has bulk capacity and option not disabled
   */
  public boolean sharedBulkCapacity() {
    return urlParser.getOptions().useBatchMultiSend;
  }

  /**
   * Permit to know if sharedConnection use compression.
   *
   * @return true if option compression is set to true
   */
  public boolean sharedUseCompression() {
    return urlParser.getOptions().useCompression;
  }

  public boolean sharedIsAurora() {
    return urlParser.isAurora();
  }

  /**
   * Check if server and client are on same host (not using containers).
   *
   * @return true if server and client are really on same host
   */
  public boolean hasSameHost() {
    try {
      Statement st = sharedConnection.createStatement();
      ResultSet rs = st.executeQuery("select @@version_compile_os");
      if (rs.next()) {
        if ((rs.getString(1).contains("linux") && Platform.isWindows())
            || (rs.getString(1).contains("win") && Platform.isLinux())) {
          return false;
        }
      }
    } catch (SQLException sqle) {
      // eat
    }
    return true;
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
    if (isGalera()) {
      ResultSet rs =
          sharedConnection.createStatement().executeQuery("show variables like '%auto_increment%'");
      while (rs.next()) {
        if ("auto_increment_increment".equals(rs.getString(1))) {
          autoInc = rs.getInt(2);
        }
        if ("auto_increment_offset".equals(rs.getString(1))) {
          autoIncOffset = rs.getInt(2);
        }
      }
      if (autoInc == 1) {
        // galera with one node only, then offset is not used
        autoIncOffset = 0;
      }
    }
    return new int[] {autoInc, autoIncOffset};
  }
}
