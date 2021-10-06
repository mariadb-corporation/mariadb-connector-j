// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.plugin.authentication.AuthenticationPlugin;
import com.singlestore.jdbc.plugin.authentication.AuthenticationPluginLoader;
import com.singlestore.jdbc.plugin.authentication.standard.NativePasswordPlugin;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class AuthenticationPluginLoaderTest extends Common {

  @Test
  public void AuthenticationPluginLoaderTest() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/");
    AuthenticationPlugin authenticationPlugin =
        AuthenticationPluginLoader.get("mysql_native_password", conf);
    assertTrue(authenticationPlugin instanceof NativePasswordPlugin);
    assertThrowsContains(
        SQLException.class,
        () -> AuthenticationPluginLoader.get("UNKNOWN", conf),
        "Client does not support authentication protocol requested by server");
  }
}
