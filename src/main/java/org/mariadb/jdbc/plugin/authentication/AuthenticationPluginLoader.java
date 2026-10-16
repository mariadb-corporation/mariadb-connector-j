// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.authentication;

import java.sql.SQLException;
import java.util.Arrays;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.NonRegisteringDriver;
import org.mariadb.jdbc.plugin.AuthenticationPluginFactory;
import org.mariadb.jdbc.util.ServiceProviders;

/** permit loading authentication plugins */
public final class AuthenticationPluginLoader {
  private static final ServiceProviders<AuthenticationPluginFactory> PROVIDERS =
      new ServiceProviders<>(
          AuthenticationPluginFactory.class, NonRegisteringDriver.class.getClassLoader());

  private AuthenticationPluginLoader() {}

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @param conf current configuration
   * @return Authentication plugin corresponding to type
   * @throws SQLException if no authentication plugin in classpath have indicated type
   */
  public static AuthenticationPluginFactory get(String type, Configuration conf)
      throws SQLException {

    String[] authList = (conf.restrictedAuth() != null) ? conf.restrictedAuth().split(",") : null;

    AuthenticationPluginFactory implClass = PROVIDERS.get(type, AuthenticationPluginFactory::type);
    if (implClass != null) {
      if (authList == null || Arrays.stream(authList).anyMatch(type::equals)) {
        return implClass;
      } else {
        throw new SQLException(
            String.format(
                "Client restrict authentication plugin to a limited set of authentication plugin"
                    + " and doesn't permit requested plugin ('%s'). Current list is"
                    + " `restrictedAuth=%s`",
                type, conf.restrictedAuth()),
            "08004",
            1251);
      }
    }
    throw new SQLException(
        "Client does not support authentication protocol requested by server. "
            + "plugin type was = '"
            + type
            + "'",
        "08004",
        1251);
  }
}
