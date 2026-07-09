// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Locale;
import java.util.Properties;
import org.junit.jupiter.api.*;

public class UnixsocketTest extends Common {

  @BeforeAll
  static void beforeAllCmd() throws SQLException {
    sharedConn
        .createStatement()
        .execute(
            "CREATE TABLE IF NOT EXISTS test_table(int_column int default 100,mediumtext_column"
                + " mediumtext null) collate = utf8mb3_bin");
  }

  @AfterAll
  static void afterAll() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IF  EXISTS test_table;");
  }

  public static long getPID() {
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    return Long.parseLong(processName.split("@")[0]);
  }

  @BeforeEach
  void setup() throws SQLException {
    sharedConn.createStatement().execute("delete from test_table");
  }

  @Test
  void ensureUnixSocketReachingBuffer() throws SQLException {
    Assumptions.assumeTrue(
        System.getenv("local") != null
            && "1".equals(System.getenv("local"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "insert into test_table (mediumtext_column) values('"
            + PreparedStatementTest.generateLongText(50000)
            + "')");
    ResultSet rs = stmt.executeQuery("select @@version_compile_os,@@socket");
    if (!rs.next() || rs.getString(2) == null) {
      return;
    }
    String path = rs.getString(2);

    try (Connection connectionSocket = createCon("&localSocket=" + path)) {
      ResultSet resultSet =
          connectionSocket.createStatement().executeQuery("select * from test_table");
      assertTrue(resultSet.next());
      assertEquals(50000, resultSet.getString("mediumtext_column").length());
      assertEquals(100, resultSet.getInt("int_column"));
    }
  }

  @Test
  public void testConnectWithUnixSocketWhenDBNotUp() throws IOException {
    Assumptions.assumeTrue(!isWindows() && !isMaxscale());

    String url = mDefUrl + "&localSocket=/tmp/not_valid_socket&localSocketAddress=localhost";

    java.sql.Driver driver = new org.mariadb.jdbc.Driver();

    Runtime rt = Runtime.getRuntime();
    // System.out.println("netstat-apnx | grep " + ProcessHandle.current().pid());
    String[] commands = {"/bin/sh", "-c", "netstat -apnx | grep " + getPID()};
    Process proc = rt.exec(commands);

    BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    int initialLines = 0;
    while (stdInput.readLine() != null) {
      initialLines++;
    }
    proc.destroy();

    for (int i = 0; i < 10; i++) {
      assertThrows(
          SQLNonTransientConnectionException.class, () -> driver.connect(url, new Properties()));
    }
    proc = rt.exec(commands);
    stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    int finalLines = 0;
    while (stdInput.readLine() != null) {
      finalLines++;
    }
    proc.destroy();
    // A file-descriptor leak would show up as the socket count growing across the 10 failed
    // connection attempts. On macOS the baseline is noisy: unrelated sockets left over from earlier
    // tests (e.g. connections in TIME_WAIT) can be reclaimed between the two measurements, making
    // the count shrink and flaking a strict equality check. There only an increase indicates a real
    // leak, so assert the count did not grow. On other platforms the baseline is stable, so keep
    // the exact-equality check for a stronger guarantee.
    if (isMac()) {
      assertTrue(
          finalLines <= initialLines,
          "Error Leaking socket file descriptors. initial :"
              + initialLines
              + " but ending with "
              + finalLines);
    } else {
      assertEquals(
          finalLines,
          initialLines,
          "Error Leaking socket file descriptors. initial :"
              + initialLines
              + " but ending with "
              + finalLines);
    }
  }

  @Test
  public void unixSocketErrorOnWindows() throws IOException {
    Assumptions.assumeTrue(isWindows());
    String url = mDefUrl + "&localSocket=/tmp/not_valid_socket&localSocketAddress=localhost";
    java.sql.Driver driver = new org.mariadb.jdbc.Driver();
    assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () -> driver.connect(url, new Properties()),
        "Unix domain sockets are not supported on Windows");
  }
}
