// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.singlestore.jdbc.integration.tools.TcpProxy;
import com.singlestore.jdbc.util.constants.HaMode;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.function.Executable;

public class Common {

  public static Connection sharedConn;
  public static Connection sharedConnBinary;
  public static String hostname;
  public static int port;
  public static String user;
  public static String password;
  public static TcpProxy proxy;
  public static String mDefUrl;
  private static Instant initialTest;

  static {
    try (InputStream inputStream =
        Common.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);
      String defaultOther;
      String val = System.getenv("TEST_REQUIRE_TLS");
      if ("1".equals(val)) {
        String cert = System.getenv("TEST_DB_SERVER_CERT");
        defaultOther = "sslMode=verify-full&serverSslCert=" + cert;
      } else {
        defaultOther = get("DB_OTHER", prop);
      }
      hostname = get("DB_HOST", prop);
      user = get("DB_USER", prop);
      port = Integer.parseInt(get("DB_PORT", prop));
      password = get("DB_PASSWORD", prop);
      mDefUrl =
          String.format(
              "jdbc:singlestore://%s:%s/%s?user=%s&password=%s&restrictedAuth=none&%s",
              hostname, port, get("DB_DATABASE", prop), user, password, defaultOther);

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
    String binUrl = mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "useServerPrepStmts=true";
    sharedConnBinary = (Connection) DriverManager.getConnection(binUrl);
  }

  @AfterAll
  public static void afterEAll() throws SQLException {
    if (sharedConn != null) {
      sharedConn.close();
      sharedConnBinary.close();
    }
    if (proxy != null) {
      proxy.forceClose();
    }
  }

  public static boolean isMariaDBServer() {
    // TODO PLAT-5820
    return false;
  }

  public static boolean minVersion(int major, int minor, int patch) {
    // TODO PLAT-5820
    try {
      return sharedConn.getMetaData().getVersion().versionGreaterOrEqual(major, minor, patch);
    } catch (SQLException e) {
      fail();
    }

    return false;
  }

  public static Connection createCon() throws SQLException {
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
      url =
          url.replaceAll(
              "jdbc:singlestore:",
              "jdbc:singlestore:" + mode.name().toLowerCase(Locale.ROOT) + ":");
    }
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    return (Connection) DriverManager.getConnection(url + opts);
  }

  public static boolean haveSsl() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("show variables like '%ssl%'");
    while (rs.next()) {
      System.out.println(rs.getString(1) + ":" + rs.getString(2));
    }

    rs = stmt.executeQuery("select @@have_ssl");
    assertTrue(rs.next());
    return "1".equals(rs.getString(1));
  }

  public static Connection createCon(String option) throws SQLException {
    return (Connection) DriverManager.getConnection(mDefUrl + "&" + option);
  }

  public static Connection createCon(String option, Integer sslPort) throws SQLException {
    Configuration conf = Configuration.parse(mDefUrl + "&" + option);
    if (sslPort != null) {
      for (HostAddress hostAddress : conf.addresses()) {
        hostAddress.port = sslPort;
      }
    }
    return Driver.connect(conf);
  }

  public static String createRowstore() {
    return minVersion(7, 3, 0) ? "CREATE ROWSTORE" : "CREATE";
  }

  @AfterEach
  public void afterEach1() throws SQLException {
    sharedConn.isValid(2000);
    sharedConnBinary.isValid(2000);
  }

  public static int getMaxAllowedPacket() throws SQLException {
    java.sql.Statement st = sharedConn.createStatement();
    ResultSet rs = st.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    return rs.getInt(1);
  }

  public void assertThrowsContains(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
  }

  public void ensureRange(Statement stmt) throws SQLException {
    stmt.execute("DROP TABLE IF EXISTS range_1_100;");
    stmt.execute("CREATE TABLE range_1_100 (n int key);");
    stmt.execute(
        "insert into range_1_100 values\n"
            + "(1), (2), (3), (4), (5), (6), (7), (8), (9), (10),\n"
            + "(11), (12), (13), (14), (15), (16), (17), (18), (19), (20),\n"
            + "(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),\n"
            + "(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),\n"
            + "(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),\n"
            + "(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),\n"
            + "(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),\n"
            + "(71), (72), (73), (74), (75), (76), (77), (78), (79), (80),\n"
            + "(81), (82), (83), (84), (85), (86), (87), (88), (89), (90),\n"
            + "(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);");
  }

  @RegisterExtension public Extension watcher = new Follow();

  private class Follow implements TestWatcher, BeforeEachCallback {
    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
      System.out.println(
          String.format(
              "  - Disabled %s: with reason :- %s",
              context.getDisplayName(), reason.orElse("No reason")));
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
      System.out.println(String.format("  - Aborted %s: ", context.getDisplayName()));
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
      System.out.println(String.format("  \u2717 Failed %s: ", context.getDisplayName()));
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
      System.out.println(
          String.format(
              "  ✓ %s: %sms",
              context.getDisplayName(), Duration.between(initialTest, Instant.now()).toMillis()));
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
      initialTest = Instant.now();
    }
  }
}
