/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.plugin.authentication;

import java.sql.SQLException;
import java.util.ServiceLoader;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.Driver;

public final class AuthenticationPluginLoader {

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @return Authentication plugin corresponding to type
   * @throws SQLException if no authentication plugin in classpath have indicated type
   */
  public static AuthenticationPlugin get(String type, Configuration conf) throws SQLException {

    ServiceLoader<AuthenticationPlugin> loader =
        ServiceLoader.load(AuthenticationPlugin.class, Driver.class.getClassLoader());

    for (AuthenticationPlugin implClass : loader) {
      if (type.equals(implClass.type())) {
        if (implClass.activeByDefault() || !conf.restrictedAuth()) {
          return implClass;
        } else {
          throw new SQLException(
              String.format(
                  "Client restrict authentication plugin to a limited set of authentication plugin and doesn't permit requested plugin ('%s'). "
                      + "This can be disabled using option `restrictedAuth=false`",
                  type),
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
