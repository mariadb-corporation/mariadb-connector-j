// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
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

  @Test
  void restrictedAuthExactMatch() throws SQLException {
    Configuration confExact =
        Configuration.parse("jdbc:mariadb://localhost/?restrictedAuth=mysql_native_password");
    assertTrue(
        AuthenticationPluginLoader.get("mysql_native_password", confExact)
            instanceof NativePasswordPlugin);

    // a plugin requested by the server must be refused unless its type is exactly listed.
    // substring entries ("mysql", "password") and empty list elements (leading/doubled comma)
    // must not let the server pick mysql_clear_password, which sends the password in clear text.
    for (String restricted :
        new String[] {
          "mysql_native_password",
          "mysql",
          "password",
          ",mysql_native_password",
          "mysql_native_password,,client_ed25519"
        }) {
      Configuration conf =
          Configuration.parse("jdbc:mariadb://localhost/?restrictedAuth=" + restricted);
      Common.assertThrowsContains(
          SQLException.class,
          () -> AuthenticationPluginLoader.get("mysql_clear_password", conf),
          "doesn't permit requested plugin");
    }
  }
}
