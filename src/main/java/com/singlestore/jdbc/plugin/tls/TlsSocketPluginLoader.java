// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.plugin.tls;

import com.singlestore.jdbc.client.tls.DefaultTlsSocketPlugin;
import java.sql.SQLException;
import java.util.ServiceLoader;

public final class TlsSocketPluginLoader {

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @return Authentication plugin corresponding to type
   * @throws SQLException if no authentication plugin in classpath have indicated type
   */
  public static TlsSocketPlugin get(String type) throws SQLException {
    if (type == null) return new DefaultTlsSocketPlugin();

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
