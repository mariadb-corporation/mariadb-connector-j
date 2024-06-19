// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.export.SslMode;

/** Host entry */
public class HostAddress {
  /** host address */
  public final String host;

  /** port */
  public final int port;

  public final String pipe;

  public final SslMode sslMode;
  public final String localSocket;

  /** primary node */
  public Boolean primary;

  private Long threadsConnected;
  private Long threadConnectedTimeout;

  /**
   * Constructor.
   *
   * @param host host
   * @param port port
   * @param primary is primary
   */
  private HostAddress(
      String host, int port, Boolean primary, String pipe, String localSocket, SslMode sslMode) {
    this.host = host;
    this.port = port;
    this.primary = primary;
    this.pipe = pipe;
    this.localSocket = localSocket;
    this.sslMode = sslMode;
  }

  /**
   * Create a Host without knowledge of primary/replica goal
   *
   * @param host host (DNS/IP)
   * @param port port
   * @return host
   */
  public static HostAddress from(String host, int port) {
    return new HostAddress(host, port, null, null, null, null);
  }

  public static HostAddress pipe(String pipe) {
    return new HostAddress(null, 3306, null, pipe, null, null);
  }

  public static HostAddress localSocket(String localSocket) {
    return new HostAddress(null, 3306, null, null, localSocket, null);
  }

  /**
   * Create a Host
   *
   * @param host host (DNS/IP)
   * @param port port
   * @param primary is primary
   * @return host
   */
  public static HostAddress from(String host, int port, boolean primary) {
    return new HostAddress(host, port, primary, null, null, null);
  }

  /**
   * Create a Host
   *
   * @param host host (DNS/IP)
   * @param port port
   * @param sslMode ssl mode
   * @return host
   */
  public static HostAddress from(String host, int port, String sslMode) {
    return new HostAddress(
        host, port, null, null, null, sslMode == null ? null : SslMode.from(sslMode));
  }

  /**
   * Create a Host
   *
   * @param host host (DNS/IP)
   * @param port port
   * @param primary is primary
   * @param sslMode ssl mode
   * @return host
   */
  public static HostAddress from(String host, int port, boolean primary, String sslMode) {
    return new HostAddress(
        host, port, primary, null, null, sslMode == null ? null : SslMode.from(sslMode));
  }

  /**
   * parse - parse server addresses from the URL fragment.
   *
   * @param spec list of endpoints in one of the forms 1 - host1,....,hostN:port (missing port
   *     default to MariaDB default 3306 2 - host:port,...,host:port
   * @param haMode High availability mode
   * @throws SQLException for wrong spec
   * @return parsed endpoints
   */
  public static List<HostAddress> parse(String spec, HaMode haMode) throws SQLException {
    if ("".equals(spec)) {
      return new ArrayList<>(0);
    }
    String[] tokens = spec.trim().split(",");
    int size = tokens.length;
    List<HostAddress> arr = new ArrayList<>(size);

    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      if (token.startsWith("address=")) {
        arr.add(parseParameterHostAddress(token, haMode, i == 0));
      } else {
        arr.add(parseSimpleHostAddress(token, haMode, i == 0));
      }
    }

    return arr;
  }

  private static HostAddress parseSimpleHostAddress(String str, HaMode haMode, boolean first)
      throws SQLException {
    String host;
    int port = 3306;

    if (str.charAt(0) == '[') {
      /* IPv6 addresses in URLs are enclosed in square brackets */
      int ind = str.indexOf(']');
      host = str.substring(1, ind);
      if (ind != (str.length() - 1) && str.charAt(ind + 1) == ':') {
        port = getPort(str.substring(ind + 2));
      }
    } else if (str.contains(":")) {
      /* Parse host:port */
      String[] hostPort = str.split(":");
      host = hostPort[0];
      port = getPort(hostPort[1]);
    } else {
      /* Just host name is given */
      host = str;
    }

    boolean primary = haMode != HaMode.REPLICATION || first;

    return new HostAddress(host, port, primary, null, null, null);
  }

  private static int getPort(String portString) throws SQLException {
    try {
      return Integer.parseInt(portString);
    } catch (NumberFormatException nfe) {
      throw new SQLException("Incorrect port value : " + portString);
    }
  }

  private static HostAddress parseParameterHostAddress(String str, HaMode haMode, boolean first)
      throws SQLException {
    String host = null;
    int port = 3306;
    String sslMode = null;
    String pipe = null;
    String localsocket = null;
    Boolean primary = null;

    String[] array = str.replace(" ", "").split("(?=\\()|(?<=\\))");
    for (int i = 1; i < array.length; i++) {
      String[] token = array[i].replace("(", "").replace(")", "").trim().split("=");
      if (token.length != 2) {
        throw new IllegalArgumentException(
            "Invalid connection URL, expected key=value pairs, found " + array[i]);
      }
      String key = token[0].toLowerCase();
      String value = token[1].toLowerCase();

      switch (key) {
        case "host":
          host = value.replace("[", "").replace("]", "");
          break;
        case "localsocket":
          localsocket = token[1];
          break;
        case "pipe":
          pipe = token[1];
          break;
        case "port":
          port = getPort(value);
          break;
        case "sslmode":
          sslMode = token[1];
          break;
        case "type":
          if ("master".equalsIgnoreCase(value) || "primary".equalsIgnoreCase(value)) {
            primary = true;
          } else if ("slave".equalsIgnoreCase(value) || "replica".equalsIgnoreCase(value)) {
            primary = false;
          } else {
            throw new SQLException(
                String.format("Wrong type value %s (possible value primary/replica)", array[i]));
          }
          break;
      }
    }

    if (primary == null) {
      if (haMode == HaMode.REPLICATION) {
        primary = first;
      } else {
        primary = true;
      }
    }

    return new HostAddress(
        host, port, primary, pipe, localsocket, sslMode == null ? null : SslMode.from(sslMode));
  }

  @Override
  public String toString() {
    if (pipe != null) return String.format("address=(pipe=%s)", pipe);
    if (localSocket != null) return String.format("address=(localSocket=%s)", localSocket);
    return String.format(
        "address=(host=%s)(port=%s)%s%s",
        host,
        port,
        (sslMode != null) ? "(sslMode=" + sslMode.getValue() + ")" : "",
        ((primary != null) ? ("(type=" + (primary ? "primary)" : "replica)")) : ""));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostAddress that = (HostAddress) o;
    return port == that.port
        && Objects.equals(host, that.host)
        && Objects.equals(primary, that.primary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, primary);
  }

  public Long getThreadsConnected() {
    return threadsConnected;
  }

  public void setThreadsConnected(long threadsConnected) {
    this.threadsConnected = threadsConnected;
    // timeout in 3 minutes
    this.threadConnectedTimeout = System.currentTimeMillis() + 3 * 60 * 1000;
  }

  public void forceThreadsConnected(long threadsConnected, long threadConnectedTimeout) {
    this.threadsConnected = threadsConnected;
    this.threadConnectedTimeout = threadConnectedTimeout;
  }

  public HostAddress withPipe(String pipe) {
    return new HostAddress(
        this.host, this.port, this.primary, pipe, this.localSocket, this.sslMode);
  }

  public HostAddress withLocalSocket(String localSocket) {
    return new HostAddress(
        this.host, this.port, this.primary, this.pipe, localSocket, this.sslMode);
  }

  public HostAddress withPort(int port) {
    return new HostAddress(
        this.host, port, this.primary, this.pipe, this.localSocket, this.sslMode);
  }

  public Long getThreadConnectedTimeout() {
    return threadConnectedTimeout;
  }
}
