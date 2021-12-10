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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.internal.failover.AbstractMastersListener;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

/**
 * Base util class. For testing example mvn test
 * -DdbUrl=jdbc:mariadb://localhost:3306,localhost:3307/test?user=root -DlogLevel=FINEST specific
 * parameters : defaultMultiHostUrl : If testing Aurora, set the region. Default is US_EAST_1.
 */
@Ignore
public class BaseMultiHostTest {

  // hosts
  private static final HashMap<HaMode, TcpProxy[]> proxySet = new HashMap<>();
  protected static String initialGaleraUrl;
  protected static String initialAuroraUrl;
  protected static String initialReplicationUrl;
  protected static String initialSequentialUrl;
  protected static String initialLoadbalanceUrl;
  protected static String initialUrl;
  protected static String proxyGaleraUrl;
  protected static String proxySequentialUrl;
  protected static String proxyAuroraUrl;
  protected static String proxyReplicationUrl;
  protected static String proxyLoadbalanceUrl;
  protected static String proxyUrl;
  protected static String jobId;
  protected static String username;
  protected static String database;
  protected static String password;
  protected static String defaultOther;
  protected static String hostname;
  protected static int port;
  public HaMode currentType;

  @Rule
  public TestRule watcher =
      new TestWatcher() {
        protected void starting(Description description) {
          System.out.println(
              "start test : " + description.getClassName() + "." + description.getMethodName());
        }

        protected void succeeded(Description description) {
          System.out.println(
              "finished test success : "
                  + description.getClassName()
                  + "."
                  + description.getMethodName());
        }

        protected void failed(Throwable throwable, Description description) {
          System.out.println(
              "finished test failed : "
                  + description.getClassName()
                  + "."
                  + description.getMethodName());
        }
      };

  protected String defaultUrl;
  protected static String mDefUrl;

  static {
    try (InputStream inputStream =
        BaseTest.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);
      String val = System.getenv("TEST_REQUIRE_TLS");
      if ("1".equals(val)) {
        String cert = System.getenv("TEST_DB_SERVER_CERT");
        defaultOther = "sslMode=verify-full&serverSslCert=" + cert;
      } else {
        defaultOther = get("DB_OTHER", prop);
      }
      hostname = get("DB_HOST", prop);
      username = get("DB_USER", prop);
      port = Integer.parseInt(get("DB_PORT", prop));
      database = get("DB_DATABASE", prop);
      password = get("DB_PASSWORD", prop);
      mDefUrl =
          String.format(
              "jdbc:mariadb://%s:%s/%s?user=%s&password=%s&restrictedAuth=none&%s",
              hostname, port, database, username, password, defaultOther);

    } catch (IOException io) {
      io.printStackTrace();
    }
  }

  private static String get(String name, Properties prop) {
    String val = System.getenv("TEST_" + name);
    if (val == null) val = System.getProperty("TEST_" + name);
    if (val == null) val = prop.getProperty(name);
    return val;
  }

  /**
   * Initialize parameters.
   *
   * @throws SQLException exception
   */
  @BeforeClass
  public static void beforeClass() throws SQLException {

    initialUrl = System.getProperty("dbFailoverUrl");
    initialGaleraUrl = System.getProperty("defaultGaleraUrl");
    initialReplicationUrl = System.getProperty("defaultReplicationUrl");
    initialLoadbalanceUrl = System.getProperty("defaultLoadbalanceUrl");
    initialSequentialUrl = System.getProperty("defaultSequentialUrl");
    initialAuroraUrl = System.getProperty("defaultAuroraUrl");
    jobId = System.getProperty("jobId", "_0");

    if (initialUrl == null && !"true".equals(System.getenv("AURORA"))) {
      String url = System.getProperty("dbUrl", mDefUrl);
      initialUrl = url.replaceAll("jdbc:mariadb://", "jdbc:mariadb:failover://");
    }
    if (initialUrl != null) {
      proxyUrl = createProxies(initialUrl, HaMode.NONE);
    }
    if (initialReplicationUrl != null) {
      proxyReplicationUrl = createProxies(initialReplicationUrl, HaMode.REPLICATION);
    }
    if (initialLoadbalanceUrl != null) {
      proxyLoadbalanceUrl = createProxies(initialLoadbalanceUrl, HaMode.LOADBALANCE);
    }
    if (initialGaleraUrl != null) {
      proxyGaleraUrl = createProxies(initialGaleraUrl, HaMode.SEQUENTIAL);
    }
    if (initialSequentialUrl != null) {
      proxySequentialUrl = createProxies(initialSequentialUrl, HaMode.SEQUENTIAL);
    }
    if (initialAuroraUrl != null) {
      proxyAuroraUrl = createProxies(initialAuroraUrl, HaMode.AURORA);
    }
  }

  /**
   * Check server minimum version.
   *
   * @param connection connection to use
   * @param major major minimal number
   * @param minor minor minimal number
   * @return is server compatible
   * @throws SQLException exception
   */
  public static boolean requireMinimumVersion(Connection connection, int major, int minor)
      throws SQLException {
    DatabaseMetaData md = connection.getMetaData();
    int dbMajor = md.getDatabaseMajorVersion();
    int dbMinor = md.getDatabaseMinorVersion();
    return (dbMajor > major || (dbMajor == major && dbMinor >= minor));
  }

  private static String createProxies(String tmpUrl, HaMode proxyType) throws SQLException {
    UrlParser tmpUrlParser;
    if (proxyType == HaMode.AURORA) {
      // if using cluster end-point, permit to retrieve current master and replica instances
      tmpUrlParser = retrieveEndpointsForProxies(tmpUrl);
    } else {
      tmpUrlParser = UrlParser.parse(tmpUrl);
    }

    TcpProxy[] tcpProxies = new TcpProxy[tmpUrlParser.getHostAddresses().size()];
    username = tmpUrlParser.getUsername();
    hostname = tmpUrlParser.getHostAddresses().get(0).host;
    StringBuilder sockethosts = new StringBuilder();
    HostAddress hostAddress;
    for (int i = 0; i < tmpUrlParser.getHostAddresses().size(); i++) {
      try {
        hostAddress = tmpUrlParser.getHostAddresses().get(i);
        tcpProxies[i] = new TcpProxy(hostAddress.host, hostAddress.port);
        sockethosts
            .append(",address=(host=localhost)(port=")
            .append(tcpProxies[i].getLocalPort())
            .append(")")
            .append((hostAddress.type != null) ? "(type=" + hostAddress.type + ")" : "");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    proxySet.put(proxyType, tcpProxies);

    String[] splitValue = tmpUrl.split("/");
    String[] subarray =
        Arrays.asList(splitValue).subList(3, splitValue.length).toArray(new String[0]);
    String dbAndParameters = String.join("/", subarray);

    if (tmpUrlParser.getHaMode().equals(HaMode.NONE)) {
      return "jdbc:mariadb://" + sockethosts.substring(1) + "/" + dbAndParameters;
    } else {
      return "jdbc:mariadb:"
          + tmpUrlParser.getHaMode().toString().toLowerCase()
          + "://"
          + sockethosts.substring(1)
          + "/"
          + dbAndParameters;
    }
  }

  private static UrlParser retrieveEndpointsForProxies(String tmpUrl) throws SQLException {
    try {
      Connection connection = DriverManager.getConnection(tmpUrl);
      connection.setReadOnly(true);
      try {
        Protocol protocol = (new BaseMultiHostTest().getProtocolFromConnection(connection));
        return protocol.getUrlParser();
      } catch (Throwable throwable) {
        connection.close();
        return UrlParser.parse(tmpUrl);
      }
    } catch (SQLException se) {
      return UrlParser.parse(tmpUrl);
    }
  }

  /** Clean proxies. */
  @AfterClass
  public static void afterClass() {
    for (TcpProxy[] tcpProxies : proxySet.values()) {
      for (TcpProxy tcpProxy : tcpProxies) {
        try {
          tcpProxy.stop();
        } catch (Exception e) {
          // Eat exception
        }
      }
    }
  }

  /** Delete table and procedure if created. Close connection if needed */
  @After
  public void afterBaseTest() {
    assureProxy();
    assureBlackList();
  }

  protected Connection getNewConnection() throws SQLException {
    return getNewConnection(null, false);
  }

  protected Connection getNewConnection(boolean proxy) throws SQLException {
    return getNewConnection(null, proxy);
  }

  protected Connection getNewConnection(String additionnalConnectionData, boolean proxy)
      throws SQLException {
    return getNewConnection(additionnalConnectionData, proxy, false);
  }

  protected Connection getNewConnection(
      String additionnalConnectionData, boolean proxy, boolean forceNewProxy) throws SQLException {
    if (proxy) {
      String tmpProxyUrl = proxyUrl;
      if (forceNewProxy) {
        tmpProxyUrl = createProxies(defaultUrl, currentType);
      }
      tmpProxyUrl += (additionnalConnectionData == null) ? "" : additionnalConnectionData;
      return DriverManager.getConnection(tmpProxyUrl);

    } else {
      if (additionnalConnectionData == null) {
        return DriverManager.getConnection(defaultUrl);
      } else {
        return DriverManager.getConnection(defaultUrl + additionnalConnectionData);
      }
    }
  }

  /**
   * Stop proxy, and restart it after a certain amount of time.
   *
   * @param hostNumber hostnumber (first is one)
   * @param millissecond milliseconds
   */
  public void stopProxy(int hostNumber, long millissecond) {
    proxySet.get(currentType)[hostNumber - 1].restart(millissecond);
  }

  /**
   * Stop proxy.
   *
   * @param hostNumber host number (first is 1)
   */
  public void stopProxy(int hostNumber) {
    proxySet.get(currentType)[hostNumber - 1].stop();
  }

  /**
   * Stop all proxy but the one in parameter.
   *
   * @param hostNumber the proxy to not close
   */
  public void stopProxyButParameter(int hostNumber) {
    TcpProxy[] proxies = proxySet.get(currentType);
    for (int i = 0; i < proxies.length; i++) {
      if (i != hostNumber - 1) {
        proxies[i].stop();
      }
    }
  }

  /**
   * Restart proxy.
   *
   * @param hostNumber host number (first is 1)
   */
  public void restartProxy(int hostNumber) {
    if (hostNumber != -1) {
      proxySet.get(currentType)[hostNumber - 1].restart();
    }
  }

  /** Assure that proxies are reset after each test. */
  public void assureProxy() {
    for (TcpProxy[] tcpProxies : proxySet.values()) {
      for (TcpProxy tcpProxy : tcpProxies) {
        tcpProxy.assureProxyOk();
      }
    }
  }

  /** Assure that blacklist is reset after each test. */
  public void assureBlackList() {
    AbstractMastersListener.clearBlacklist();
  }

  /** Does the user have super privileges or not. */
  public boolean hasSuperPrivilege(Connection connection, String testName) throws SQLException {
    boolean superPrivilege = false;
    Statement st = connection.createStatement();

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

    if (superPrivilege) {
      System.out.println(
          "test '" + testName + "' skipped because user '" + username + "' has SUPER privileges");
    }

    return superPrivilege;
  }

  protected Protocol getProtocolFromConnection(Connection conn) throws Throwable {

    Method getProtocol = MariaDbConnection.class.getDeclaredMethod("getProtocol");
    getProtocol.setAccessible(true);
    return (Protocol) getProtocol.invoke(conn);
  }

  /**
   * Retrieve server Id.
   *
   * @param connection connection
   * @return server index
   * @throws Throwable exception
   */
  public int getServerId(Connection connection) throws Throwable {
    Protocol protocol = getProtocolFromConnection(connection);
    HostAddress hostAddress = protocol.getHostAddress();
    List<HostAddress> hostAddressList = protocol.getUrlParser().getHostAddresses();
    return hostAddressList.indexOf(hostAddress) + 1;
  }

  /**
   * Retrieve current HostAddress.
   *
   * @param connection connection
   * @return Current Host address
   * @throws Throwable if any exception occur
   */
  public HostAddress getServerHostAddress(Connection connection) throws Throwable {
    Protocol protocol = getProtocolFromConnection(connection);
    return protocol.getHostAddress();
  }

  public boolean inTransaction(Connection connection) throws Throwable {
    Protocol protocol = getProtocolFromConnection(connection);
    return protocol.inTransaction();
  }

  protected boolean isMariaDbServer(Connection connection) throws SQLException {
    DatabaseMetaData md = connection.getMetaData();
    return md.getDatabaseProductVersion().contains("MariaDB");
  }

  protected ServerPrepareResult getPrepareResult(ServerSidePreparedStatement preparedStatement)
      throws IllegalAccessException, NoSuchFieldException {
    Field prepareResultField =
        ServerSidePreparedStatement.class.getDeclaredField("serverPrepareResult");
    prepareResultField.setAccessible(true);
    return (ServerPrepareResult)
        prepareResultField.get(preparedStatement); // IllegalAccessException
  }
}
