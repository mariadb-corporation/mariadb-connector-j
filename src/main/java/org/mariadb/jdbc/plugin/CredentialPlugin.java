// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin;

import java.sql.SQLException;
import java.util.function.Supplier;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;

/** Credential plugin definition, to permit providing Credential to server */
public interface CredentialPlugin extends Supplier<Credential> {
  /**
   * credential identifier
   *
   * @return type
   */
  String type();

  /**
   * Indicate if plugin must throw an error if SSL is not enabled
   *
   * @return if ssl is required
   */
  default boolean mustUseSsl() {
    return false;
  }

  /**
   * Indicate authentication plugin type to use for authentication
   *
   * @return plugin type to use for authentication, or null for default
   */
  default String defaultAuthenticationPluginType() {
    return null;
  }

  /**
   * Permit initializing plugin if overridden
   *
   * @param conf configuration
   * @param userName user
   * @param hostAddress host information
   * @return credential plugin
   * @throws SQLException if any error occurs
   */
  default CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress)
      throws SQLException {
    return this;
  }
}
