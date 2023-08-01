// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.integration.Common;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.mariadb.jdbc.plugin.authentication.standard.NativePasswordPlugin;

public class AuthenticationPluginLoaderTest extends Common {

  @Test
  public void authenticationPluginLoaderTest() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/");
    AuthenticationPlugin authenticationPlugin =
        AuthenticationPluginLoader.get("mysql_native_password", conf);
    assertTrue(authenticationPlugin instanceof NativePasswordPlugin);
    Common.assertThrowsContains(
        SQLException.class,
        () -> AuthenticationPluginLoader.get("UNKNOWN", conf),
        "Client does not support authentication protocol requested by server");
  }
}
