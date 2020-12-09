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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.mariadb.jdbc.util.constants.HaMode;

public class HostAddress {

  public String host;
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
    if (spec == null) {
      throw new IllegalArgumentException("Invalid connection URL, host address must not be empty ");
    }
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

  private static HostAddress parseSimpleHostAddress(String str, HaMode haMode, boolean first) {
    String host;
    int port = 3306;
    Boolean primary = null;

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

    if (primary == null) {
      switch (haMode) {
        case REPLICATION:
          primary = first;
          break;

        default:
          primary = true;
          break;
      }
    }

    return new HostAddress(host, port, primary);
  }

  private static int getPort(String portString) {
    try {
      return Integer.parseInt(portString);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Incorrect port value : " + portString);
    }
  }

  private static HostAddress parseParameterHostAddress(String str, HaMode haMode, boolean first)
      throws SQLException {
    String host = null;
    int port = 3306;
    Boolean primary = null;

    String[] array = str.split("(?=\\()|(?<=\\))");
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
      switch (haMode) {
        case REPLICATION:
          primary = first;
          break;

        default:
          primary = true;
          break;
      }
    }

    return new HostAddress(host, port, primary);
  }

  /**
   * ToString implementation of addresses.
   *
   * @param addrs address list
   * @return String value
   */
  public static String toString(List<HostAddress> addrs) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < addrs.size(); i++) {
      if (addrs.get(i).primary != null) {
        str.append("address=(host=")
            .append(addrs.get(i).host)
            .append(")(port=")
            .append(addrs.get(i).port)
            .append(")(type=")
            .append(addrs.get(i).primary ? "primary" : "replica")
            .append(")");
      } else {
        boolean isIPv6 = addrs.get(i).host != null && addrs.get(i).host.contains(":");
        String host = (isIPv6) ? ("[" + addrs.get(i).host + "]") : addrs.get(i).host;
        str.append(host).append(":").append(addrs.get(i).port);
      }
      if (i < addrs.size() - 1) {
        str.append(",");
      }
    }
    return str.toString();
  }

  /**
   * ToString implementation of addresses.
   *
   * @param addrs address array
   * @return String value
   */
  @SuppressWarnings("unused")
  public static String toString(HostAddress[] addrs) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < addrs.length; i++) {
      if (addrs[i].primary != null) {
        str.append("address=(host=")
            .append(addrs[i].host)
            .append(")(port=")
            .append(addrs[i].port)
            .append(")(type=")
            .append(addrs[i].primary ? "primary" : "replica")
            .append(")");
      } else {
        boolean isIPv6 = addrs[i].host != null && addrs[i].host.contains(":");
        String host = (isIPv6) ? ("[" + addrs[i].host + "]") : addrs[i].host;
        str.append(host).append(":").append(addrs[i].port);
      }
      if (i < addrs.length - 1) {
        str.append(",");
      }
    }
    return str.toString();
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
