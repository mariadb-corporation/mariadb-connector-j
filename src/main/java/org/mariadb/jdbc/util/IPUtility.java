// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.util;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPUtility {
  /*
  Check that host is an IP address or not, trying to avoid DNS resolution
   */
  public static boolean isInetAddress(String ipString) {
    if (ipString == null || ipString.isEmpty()) {
      return false;
    }

    // Reject anything that could trigger hostname resolution.
    // Allow only characters that can appear in numeric IP literals (+ optional IPv6 scope).
    // Note: we intentionally do not validate scope IDs against local interfaces.
    for (int i = 0; i < ipString.length(); i++) {
      char c = ipString.charAt(i);
      boolean ok =
          (c >= '0' && c <= '9')
              || (c >= 'a' && c <= 'f')
              || (c >= 'A' && c <= 'F')
              || c == '.'
              || c == ':'
              || c == '%';
      if (!ok) {
        return false;
      }
    }

    int percent = ipString.indexOf('%');
    String literal = (percent == -1) ? ipString : ipString.substring(0, percent);
    if (literal.isEmpty()) {
      return false;
    }

    // IPv4 (no scope allowed).
    if (literal.indexOf(':') == -1) {
      if (percent != -1) {
        return false;
      }
      String[] parts = literal.split("\\.", -1);
      if (parts.length != 4) {
        return false;
      }
      for (String part : parts) {
        if (part.isEmpty() || part.length() > 3) {
          return false;
        }
        // Disallow leading zeros ("01") to match existing strict parsing behavior.
        if (part.length() > 1 && part.charAt(0) == '0') {
          return false;
        }
        int value = 0;
        for (int i = 0; i < part.length(); i++) {
          char c = part.charAt(i);
          if (c < '0' || c > '9') {
            return false;
          }
          value = value * 10 + (c - '0');
        }
        if (value > 255) {
          return false;
        }
      }
      return true;
    }

    // IPv6 (optional scope allowed). Delegate numeric parsing to the JDK without DNS.
    // With the character filter above, this cannot be a hostname.
    try {
      return InetAddress.getByName(literal) instanceof Inet6Address;
    } catch (UnknownHostException e) {
      return false;
    }
  }

  /**
   * Remove the DNS root label marker from a hostname. A trailing dot marks a name as absolute for
   * resolution only: it must be kept for DNS resolution, but removed for every TLS use of the name.
   * RFC 6066 section 3 prohibits it in SNI, and certificates never carry it, so an IP literal
   * written as {@code 10.0.0.1.} must be normalized before being recognized as an IP.
   *
   * @param host hostname, may be null
   * @return hostname without any trailing dot
   */
  public static String stripTrailingDot(String host) {
    if (host == null) {
      return null;
    }
    int end = host.length();
    while (end > 0 && host.charAt(end - 1) == '.') {
      end--;
    }
    return end == host.length() ? host : host.substring(0, end);
  }
}
