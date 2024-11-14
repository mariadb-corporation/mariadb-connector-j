// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.integration.Common;
import org.mariadb.jdbc.plugin.AuthenticationPluginFactory;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.mariadb.jdbc.plugin.authentication.standard.NativePasswordPluginFactory;

public class AuthenticationPluginLoaderTest extends Common {

  @Test
  public void authenticationPluginLoaderTest() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/");
    AuthenticationPluginFactory authenticationPluginFactory =
        AuthenticationPluginLoader.get("mysql_native_password", conf);
    assertTrue(authenticationPluginFactory instanceof NativePasswordPluginFactory);
    Common.assertThrowsContains(
        SQLException.class,
        () -> AuthenticationPluginLoader.get("UNKNOWN", conf),
        "Client does not support authentication protocol requested by server");
  }
}
