// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc;

import com.singlestore.jdbc.util.constants.HaMode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HostAddress {

  public final String host;
  public int port;
  public Boolean primary;

  /**
   * Constructor.
   *
   * @param host host
   * @param port port
   * @param primary is primary
   */
  private HostAddress(String host, int port, Boolean primary) {
    this.host = host;
    this.port = port;
    this.primary = primary;
  }

  public static HostAddress from(String host, int port) {
    return new HostAddress(host, port, null);
  }

  public static HostAddress from(String host, int port, boolean primary) {
    return new HostAddress(host, port, primary);
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

    return new HostAddress(host, port, primary);
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
        case "port":
          port = getPort(value);
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

    return new HostAddress(host, port, primary);
  }

  @Override
  public String toString() {
    return String.format(
        "address=(host=%s)(port=%s)%s",
        host, port, ((primary != null) ? ("(type=" + (primary ? "primary)" : "replica)")) : ""));
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
}
