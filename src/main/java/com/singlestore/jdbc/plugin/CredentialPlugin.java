// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import java.sql.SQLException;

public interface CredentialPlugin {

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

  Credential get() throws SQLException;

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
