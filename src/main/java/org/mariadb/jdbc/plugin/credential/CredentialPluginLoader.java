// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.credential;

import java.sql.SQLException;
import java.util.ServiceLoader;
import org.mariadb.jdbc.Driver;
import org.mariadb.jdbc.plugin.CredentialPlugin;

/**
 * Provider to handle plugin authentication. This can allow library users to override our default
 * Authentication provider.
 */
public final class CredentialPluginLoader {

  /**
   * Get current Identity plugin according to option `identityType`.
   *
   * @param type identity plugin type
   * @return identity plugin
   * @throws SQLException if no identity plugin found with this type is in classpath
   */
  public static CredentialPlugin get(String type) throws SQLException {
    if (type == null) return null;

    ServiceLoader<CredentialPlugin> loader =
        ServiceLoader.load(CredentialPlugin.class, Driver.class.getClassLoader());

    for (CredentialPlugin implClass : loader) {
      if (type.equals(implClass.type())) {
        return implClass;
      }
    }
    throw new SQLException(
        "No identity plugin registered with the type \"" + type + "\".", "08004", 1251);
  }
}
