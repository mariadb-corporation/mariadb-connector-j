// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.addon;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.AuthenticationPluginFactory;

/** Clear password plugin. */
public class ClearPasswordPluginFactory implements AuthenticationPluginFactory {

  @Override
  public String type() {
    return "mysql_clear_password";
  }

  @Override
  public boolean requireSsl() {
    return true;
  }

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection string options
   * @param hostAddress host information
   */
  public AuthenticationPlugin initialize(
      String authenticationData, byte[] seed, Configuration conf, HostAddress hostAddress) {
    return new ClearPasswordPlugin(authenticationData);
  }
}
