// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
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
   * Authentication plugin required SSL to be used
   *
   * @return true if SSL is required
   */
  default boolean requireSsl() {
    return false;
  }
}
