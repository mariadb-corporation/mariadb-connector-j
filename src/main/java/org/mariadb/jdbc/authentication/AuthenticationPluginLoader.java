/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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

package org.mariadb.jdbc.authentication;

import java.sql.SQLException;
import java.util.ServiceLoader;

/**
 * Provider to handle plugin authentication. This can allow library users to override our default
 * Authentication provider.
 */
public class AuthenticationPluginLoader {

  public static AuthenticationPlugin get(String key) throws SQLException {
    if (key == null || key.isEmpty()) return null;
    ServiceLoader<AuthenticationPlugin> loader = ServiceLoader.load(AuthenticationPlugin.class);
    for (AuthenticationPlugin implClass : loader) {
      if (key.equals(implClass.type())) {
        return implClass;
      }
    }
    throw new SQLException(
        "Client does not support authentication protocol requested by server. "
            + "Consider upgrading MariaDB client. plugin was = "
            + key,
        "08004",
        1251);
  }


}
