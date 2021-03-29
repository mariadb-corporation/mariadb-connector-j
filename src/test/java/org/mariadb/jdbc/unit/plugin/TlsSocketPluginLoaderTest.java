package org.mariadb.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.client.tls.DefaultTlsSocketPlugin;
import org.mariadb.jdbc.plugin.tls.TlsSocketPlugin;
import org.mariadb.jdbc.plugin.tls.TlsSocketPluginLoader;

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
