// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;

/** Authentication plugin descriptor */
public interface AuthenticationPluginFactory {

  /**
   * Authentication plugin type.
   *
   * @return authentication plugin type. ex: mysql_native_password
   */
  String type();

  /**
   * Plugin initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection options
   * @param hostAddress host address
   */
  AuthenticationPlugin initialize(
      String authenticationData, byte[] seed, Configuration conf, HostAddress hostAddress);

  /**
   * Whether this authentication plugin requires a secure connection. Plugins that transmit the
   * password (or other secret) in clear text return {@code true}; the driver then only runs them
   * over a secure transport (TLS, or a local unix socket), never over plain TCP.
   *
   * @return true if a secure connection is required
   */
  default boolean requireSecure() {
    return false;
  }
}
