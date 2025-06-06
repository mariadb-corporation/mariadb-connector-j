package org.mariadb.jdbc;
// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class UnixsocketTest extends BaseTest {
  @Test
  public void testConnectWithUnixSocketWhenDBNotUp() throws IOException {
    Assume.assumeTrue(!System.getProperty("os.name").toLowerCase().contains("win") && !System.getProperty("os.name").toLowerCase().contains("mac"));

    String jdbcBase =
        "jdbc:mariadb://%s:%s/%s?user=%s&password=%s&sslMode=DISABLED&useServerPrepStmts=%s&cachePrepStmts=%s&serverTimezone=UTC&trackSessionState=TRUE%s";
    String url =
        String.format(
            jdbcBase,
            hostname,
            port,
            database,
            username,
            password,
            false,
            false,
            "&localSocket=/tmp/not_valid_socket&localSocketAddress=localhost");

    java.sql.Driver driver = new org.mariadb.jdbc.Driver();

    Runtime rt = Runtime.getRuntime();
    String pid = ManagementFactory.getRuntimeMXBean().getName();
    System.out.println(pid);
    if (pid.indexOf("@") >= 0) {
      pid = pid.substring(0, pid.indexOf("@"));
    }
    System.out.println(pid);
    System.out.println("netstat-apnx | grep " + pid);
    String[] commands = {"/bin/sh", "-c", "netstat -apnx | grep " + pid};
    Process proc = rt.exec(commands);

    BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    int initialLines = 0;
    while (stdInput.readLine() != null) {
      initialLines++;
    }
    proc.destroy();

    for (int i = 0; i < 10; i++) {
      Assert.assertThrows(
          SQLNonTransientConnectionException.class,
          () -> {
            driver.connect(url, new Properties());
          });
    }
    proc = rt.exec(commands);
    stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    int finalLines = 0;
    while (stdInput.readLine() != null) {
      finalLines++;
    }
    proc.destroy();
    Assert.assertEquals(
        "Error Leaking socket file descriptors. initial :"
            + initialLines
            + " but ending with "
            + finalLines,
        finalLines,
        initialLines);
  }
}
