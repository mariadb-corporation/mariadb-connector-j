// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.AuthenticationPluginFactory;

/** Native password implementation */
public class NativePasswordPluginFactory implements AuthenticationPluginFactory {

  @Override
  public String type() {
    return "mysql_native_password";
  }

  public AuthenticationPlugin initialize(
      String authenticationData, byte[] seed, Configuration conf, HostAddress hostAddress) {
    return new NativePasswordPlugin(authenticationData, seed);
  }
}
