// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.tidb.jdbc.Configuration;
import org.tidb.jdbc.integration.Common;
import org.tidb.jdbc.plugin.AuthenticationPlugin;
import org.tidb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.tidb.jdbc.plugin.authentication.standard.NativePasswordPlugin;

public class AuthenticationPluginLoaderTest extends Common {

  @Test
  public void authenticationPluginLoaderTest() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:tidb://localhost/");
    AuthenticationPlugin authenticationPlugin =
        AuthenticationPluginLoader.get("mysql_native_password", conf);
    assertTrue(authenticationPlugin instanceof NativePasswordPlugin);
    Common.assertThrowsContains(
        SQLException.class,
        () -> AuthenticationPluginLoader.get("UNKNOWN", conf),
        "Client does not support authentication protocol requested by server");
  }
}
