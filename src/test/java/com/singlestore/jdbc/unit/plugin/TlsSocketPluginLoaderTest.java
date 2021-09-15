// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.client.tls.DefaultTlsSocketPlugin;
import com.singlestore.jdbc.plugin.tls.TlsSocketPlugin;
import com.singlestore.jdbc.plugin.tls.TlsSocketPluginLoader;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class TlsSocketPluginLoaderTest extends Common {

  @Test
  public void AuthenticationPluginLoaderTest() throws SQLException {
    TlsSocketPlugin tlsSocketPlugin = TlsSocketPluginLoader.get("DEFAULT");
    assertTrue(tlsSocketPlugin instanceof DefaultTlsSocketPlugin);
    assertThrowsContains(
        SQLException.class,
        () -> TlsSocketPluginLoader.get("UNKNOWN"),
        "Client has not found any TLS factory plugin with name 'UNKNOWN'");
  }
}
