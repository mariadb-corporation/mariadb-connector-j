// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.integration.Common;

/**
 * Connection to a host resolving to several addresses, against the test server: each address is
 * tried in turn, within the connectTimeout budget. The configured test host is resolved to get the
 * server's addresses, and unreachable addresses are put in front of them.
 */
public class ConnectionHelperAddressesTest extends Common {

  private static InetAddress ip(String literal) throws IOException {
    return InetAddress.getByName(literal);
  }

  private static Configuration conf(int connectTimeout) throws SQLException {
    return Configuration.parse(mDefUrl + "&connectTimeout=" + connectTimeout);
  }

  private static List<InetAddress> serverAddresses() throws IOException {
    return Arrays.asList(InetAddress.getAllByName(hostname));
  }

  private static InetAddress[] prepend(InetAddress first, List<InetAddress> rest) {
    InetAddress[] addresses = new InetAddress[rest.size() + 1];
    addresses[0] = first;
    for (int i = 0; i < rest.size(); i++) addresses[i + 1] = rest.get(i);
    return addresses;
  }

  /** The server sends its initial handshake on accept: packet header, then protocol version 10. */
  private static void assertServerHandshake(Socket socket) throws IOException {
    socket.setSoTimeout(5_000);
    byte[] start = socket.getInputStream().readNBytes(5);
    assertEquals(5, start.length, "handshake packet expected");
    assertEquals(10, start[4], "protocol version");
  }

  @Test
  public void attemptTimeoutSharesTheBudget() {
    // no connect timeout: no attempt timeout either
    assertEquals(0, ConnectionHelper.attemptTimeout(0, 30_000, 2));
    // single address: whole budget
    assertEquals(30_000, ConnectionHelper.attemptTimeout(30_000, 30_000, 1));
    // two addresses: half each, rounded up
    assertEquals(15_000, ConnectionHelper.attemptTimeout(30_000, 30_000, 2));
    assertEquals(10_001, ConnectionHelper.attemptTimeout(30_000, 30_001, 3));
    // unused time of earlier attempts goes to the last one
    assertEquals(29_000, ConnectionHelper.attemptTimeout(30_000, 29_000, 1));
    // never more than connectTimeout, never 0 once a timeout is configured
    assertEquals(30_000, ConnectionHelper.attemptTimeout(30_000, 40_000, 1));
    assertEquals(1, ConnectionHelper.attemptTimeout(30_000, 0, 2));
    assertEquals(1, ConnectionHelper.attemptTimeout(30_000, -5, 1));
  }

  @Test
  public void fallsThroughUnreachableAddressToServer() throws Exception {
    Configuration conf = conf(2_000);
    HostAddress host = conf.addresses().get(0);
    List<InetAddress> server = serverAddresses();
    // TEST-NET-1 is not routed: the attempt times out after its share of the budget, or is
    // rejected at once by the local network. Either way the server addresses must be tried next.
    InetAddress[] addresses = prepend(ip("192.0.2.1"), server);

    long start = System.nanoTime();
    InetAddress connected;
    try (Socket socket = ConnectionHelper.connectSocket(conf, host, addresses)) {
      connected = socket.getInetAddress();
      assertTrue(server.contains(connected), "connected to " + connected);
      assertServerHandshake(socket);
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertTrue(elapsedMs < 1_900, "unreachable address must only get its share: " + elapsedMs);

    // the address that worked is remembered, and used first by the next connection to the host
    assertEquals(connected, ConnectionHelper.LAST_SUCCESSFUL_ADDRESS.get(hostname));
    try (Socket socket = ConnectionHelper.connectSocket(conf, host)) {
      assertEquals(connected, socket.getInetAddress());
      assertServerHandshake(socket);
    }
  }

  @Test
  public void allAddressesFailing() throws Exception {
    int closedPort;
    try (ServerSocket server = new ServerSocket(0)) {
      closedPort = server.getLocalPort();
    }
    Configuration conf = conf(3_000);
    HostAddress host = conf.addresses().get(0).withPort(closedPort);
    List<InetAddress> server = serverAddresses();
    InetAddress[] addresses = server.toArray(new InetAddress[0]);

    SQLException e =
        assertThrows(
            SQLException.class, () -> ConnectionHelper.connectSocket(conf, host, addresses));
    assertEquals("08000", e.getSQLState());
    // the thrown exception is the last attempt, earlier ones are kept as suppressed
    InetAddress last = server.get(server.size() - 1);
    assertTrue(e.getMessage().contains("[" + last.getHostAddress() + "]"), e.getMessage());
    assertEquals(server.size() - 1, e.getSuppressed().length);
  }

  @Test
  public void budgetBoundsTheWholeLoop() throws Exception {
    Configuration conf = conf(400);
    HostAddress host = conf.addresses().get(0);
    InetAddress[] addresses = {ip("192.0.2.1"), ip("192.0.2.2"), ip("192.0.2.3")};

    long start = System.nanoTime();
    SQLException e =
        assertThrows(
            SQLException.class, () -> ConnectionHelper.connectSocket(conf, host, addresses));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertNotNull(e.getMessage());
    assertTrue(elapsedMs < 1_500, "three attempts must share the 400ms budget: " + elapsedMs);
  }

  @Test
  public void noAddress() throws Exception {
    Configuration conf = conf(400);
    HostAddress host = conf.addresses().get(0);
    SQLNonTransientConnectionException e =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> ConnectionHelper.connectSocket(conf, host, new InetAddress[0]));
    assertTrue(e.getMessage().contains("No address"), e.getMessage());
  }

  /**
   * Real dual-stack test host: the JDBC connection must succeed whichever address the server
   * listens on.
   */
  @Test
  public void multiAddressTestHost() throws Exception {
    List<InetAddress> server = serverAddresses();
    Assumptions.assumeTrue(server.size() > 1, "test host resolves to a single address");
    try (Connection con = (Connection) DriverManager.getConnection(mDefUrl)) {
      assertTrue(con.isValid(5));
    }
    InetAddress remembered = ConnectionHelper.LAST_SUCCESSFUL_ADDRESS.get(hostname);
    assertNotNull(remembered);
    assertTrue(server.contains(remembered));
  }
}
