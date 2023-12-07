// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential;

import com.singlestore.jdbc.Driver;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Provider to handle plugin authentication. This can allow library users to override our default
 * Authentication provider.
 */
public final class CredentialPluginLoader {

  private static final ServiceLoader<CredentialPlugin> loader =
      ServiceLoader.load(CredentialPlugin.class, Driver.class.getClassLoader());

  /**
   * Get current Identity plugin according to option `identityType`.
   *
   * @param type identity plugin type
   * @return identity plugin
   * @throws SQLException if no identity plugin found with this type is in classpath
   */
  public static CredentialPlugin get(String type) throws SQLException {
    if (type == null) return null;

    Iterator<CredentialPlugin> iter = loader.iterator();
    StringWriter errors = new StringWriter();
    PrintWriter errorsWriter = new PrintWriter(errors);

    CredentialPlugin implClass = null;
    do {
      if (!iter.hasNext()) {
        String msg = errors.toString();
        if (msg.length() > 0) {
          throw new SQLException(
              "No identity plugin registered with the type \""
                  + type
                  + "\" "
                  + "or the required plugin could not be loaded. "
                  + "Some plugins failed to load:\n"
                  + msg,
              "08004",
              1251);
        } else {
          throw new SQLException(
              "No identity plugin registered with the type \"" + type + "\"", "08004", 1251);
        }
      }

      try {
        implClass = iter.next();
      } catch (ServiceConfigurationError e) {
        errorsWriter.println(
            "Could not load credential plugin. Please verify that"
                + " all optional packages required for this plugin are installed:");
        e.printStackTrace(errorsWriter);
      }
    } while (implClass == null || !type.equals(implClass.type()));

    return implClass;
  }
}
