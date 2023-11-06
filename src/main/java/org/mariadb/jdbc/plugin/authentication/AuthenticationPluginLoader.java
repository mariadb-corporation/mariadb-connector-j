// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.ServiceLoader;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.Driver;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;

/** permit loading authentication plugins */
public final class AuthenticationPluginLoader {

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @param conf current configuration
   * @return Authentication plugin corresponding to type
   * @throws SQLException if no authentication plugin in classpath have indicated type
   */
  public static AuthenticationPlugin get(String type, Configuration conf) throws SQLException {

    ServiceLoader<AuthenticationPlugin> loader =
        ServiceLoader.load(AuthenticationPlugin.class, Driver.class.getClassLoader());

    String[] authList = (conf.restrictedAuth() != null) ? conf.restrictedAuth().split(",") : null;

    for (AuthenticationPlugin implClass : loader) {
      if (type.equals(implClass.type())) {
        if (authList == null || Arrays.stream(authList).anyMatch(type::contains)) {
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
