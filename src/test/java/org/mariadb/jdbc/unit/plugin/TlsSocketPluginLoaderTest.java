// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.integration.Common;
import org.mariadb.jdbc.plugin.TlsSocketPlugin;
import org.mariadb.jdbc.plugin.tls.TlsSocketPluginLoader;
import org.mariadb.jdbc.plugin.tls.main.DefaultTlsSocketPlugin;

public class TlsSocketPluginLoaderTest extends Common {

  @Test
  public void AuthenticationPluginLoaderTest() throws SQLException {
    TlsSocketPlugin tlsSocketPlugin = TlsSocketPluginLoader.get("DEFAULT");
    assertTrue(tlsSocketPlugin instanceof DefaultTlsSocketPlugin);
    Common.assertThrowsContains(
        SQLException.class,
        () -> TlsSocketPluginLoader.get("UNKNOWN"),
        "Client has not found any TLS factory plugin with name 'UNKNOWN'");
  }
}
