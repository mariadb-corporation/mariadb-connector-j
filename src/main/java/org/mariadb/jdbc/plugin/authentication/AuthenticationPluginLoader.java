// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

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
    String restrictedAuth =
        conf.restrictedAuth() == null
            ? "mysql_native_password,client_ed25519,auth_gssapi_client"
            : conf.restrictedAuth();
    String[] authList = restrictedAuth.split(",");
    for (AuthenticationPlugin implClass : loader) {
      if (type.equals(implClass.type())) {
        if ("none".equals(restrictedAuth) || Arrays.stream(authList).anyMatch(type::contains)) {
          return implClass;
        } else {
          throw new SQLException(
              String.format(
                  "Client restrict authentication plugin to a limited set of authentication plugin and doesn't permit requested plugin ('%s'). "
                      + "Current list is `restrictedAuth=%s`",
                  type, restrictedAuth),
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
