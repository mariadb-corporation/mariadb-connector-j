// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.tls;

import java.sql.SQLException;
import org.mariadb.jdbc.NonRegisteringDriver;
import org.mariadb.jdbc.plugin.TlsSocketPlugin;
import org.mariadb.jdbc.plugin.tls.main.DefaultTlsSocketPlugin;
import org.mariadb.jdbc.util.ServiceProviders;

/** TLS plugin loader */
public final class TlsSocketPluginLoader {
  private static final ServiceProviders<TlsSocketPlugin> PROVIDERS =
      new ServiceProviders<>(TlsSocketPlugin.class, NonRegisteringDriver.class.getClassLoader());

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

    TlsSocketPlugin implClass = PROVIDERS.get(type, TlsSocketPlugin::type);
    if (implClass != null) {
      return implClass;
    }
    throw new SQLException(
        "Client has not found any TLS factory plugin with name '" + type + "'.", "08004", 1251);
  }
}
