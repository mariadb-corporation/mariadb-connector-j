package org.mariadb.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.mariadb.jdbc.plugin.authentication.standard.NativePasswordPlugin;

public class AuthenticationPluginLoaderTest extends Common {

  @Test
  public void AuthenticationPluginLoaderTest() throws SQLException {
    AuthenticationPlugin authenticationPlugin =
        AuthenticationPluginLoader.get("mysql_native_password");
    assertTrue(authenticationPlugin instanceof NativePasswordPlugin);
    assertThrowsContains(
        SQLException.class,
        () -> AuthenticationPluginLoader.get("UNKNOWN"),
        "Client does not support authentication protocol requested by server");
  }
}
