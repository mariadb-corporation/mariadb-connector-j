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

package org.mariadb.jdbc.plugin.tls;

import java.sql.SQLException;
import java.util.ServiceLoader;
import org.mariadb.jdbc.client.tls.DefaultTlsSocketPlugin;

public class TlsSocketPluginLoader {

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @return Authentication plugin corresponding to type
   * @throws SQLException if no authentication plugin in classpath have indicated type
   */
  public static TlsSocketPlugin get(String type) throws SQLException {
    if (type == null || type.isEmpty()) {
      return new DefaultTlsSocketPlugin();
    }
    ServiceLoader<TlsSocketPlugin> loader = ServiceLoader.load(TlsSocketPlugin.class);
    for (TlsSocketPlugin implClass : loader) {
      if (type.equals(implClass.type())) {
        return implClass;
      }
    }
    throw new SQLException(
        "Client has not found any TLS factory plugin with name '" + type + "'.", "08004", 1251);
  }
}
