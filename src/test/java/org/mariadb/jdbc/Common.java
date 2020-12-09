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

package org.mariadb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.function.Executable;
import org.mariadb.jdbc.integration.tools.TcpProxy;
import org.mariadb.jdbc.util.constants.HaMode;

public class Common {

  public static Connection sharedConn;
  public static TcpProxy proxy;
  public static String mDefUrl;
  private static Instant initialTest;

  static {
    try (InputStream inputStream =
        Common.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);
      mDefUrl =
          String.format(
              "jdbc:mariadb://%s:%s/%s?user=%s&password=%s&%s",
              get("DB_HOST", prop),
              get("DB_PORT", prop),
              get("DB_DATABASE", prop),
              get("DB_USER", prop),
              get("DB_PASSWORD", prop),
              get("DB_OTHER", prop));
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

  @BeforeAll
  public static void beforeAll() throws Exception {
    sharedConn = (Connection) DriverManager.getConnection(mDefUrl);
  }

  @AfterAll
  public static void afterEAll() throws SQLException {
    sharedConn.close();
  }

  public static boolean isMariaDBServer() {
    return sharedConn.getContext().getVersion().isMariaDBServer();
  }

  public static boolean minVersion(int major, int minor, int patch) {
    return sharedConn.getContext().getVersion().versionGreaterOrEqual(major, minor, patch);
  }

  public Connection createCon() throws SQLException {
    return (Connection) DriverManager.getConnection(mDefUrl);
  }

  public Connection createProxyCon(HaMode mode, String opts) throws SQLException {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url = mDefUrl.replaceAll("//([^/]*)/", "//localhost:" + proxy.getLocalPort() + "/");
    if (mode != HaMode.NONE) {
      url = url.replaceAll("jdbc:mariadb:", "jdbc:mariadb:sequential:");
    }
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    return (Connection) DriverManager.getConnection(url + opts);
  }

  public void cancelForVersion(int major, int minor) {
    String dbVersion = sharedConn.getMetaData().getDatabaseProductVersion();
    Assumptions.assumeFalse(dbVersion.startsWith(major + "." + minor));
  }

  //  @RegisterExtension public Extension watcher = new Follow();

  public Connection createCon(String option) throws SQLException {
    return (Connection) DriverManager.getConnection(mDefUrl + "&" + option);
  }

  @AfterEach
  public void afterEach1() throws SQLException {
    sharedConn.isValid(2000);
  }

  //  public boolean haveSsl(MariadbConnection connection) {
  //    return connection
  //        .createStatement("select @@have_ssl")
  //        .execute()
  //        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
  //        .blockLast()
  //        .equals("YES");
  //  }

  public void assertThrowsContains(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
  }

  private class Follow implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
      System.out.println(Duration.between(initialTest, Instant.now()).toString());
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
      initialTest = Instant.now();
      System.out.print("       test : " + extensionContext.getTestMethod().get() + " ");
    }
  }
}
