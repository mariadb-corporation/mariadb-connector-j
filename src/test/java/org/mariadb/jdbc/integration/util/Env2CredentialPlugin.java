// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.util;

import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.CredentialPlugin;

/**
 * Authentication using environment variable.
 *
 * <p>default implementation use environment variable MARIADB_USER and MARIADB_PWD
 *
 * <p>example : `jdbc:mariadb://host/db?credentialType=ENV`
 *
 * <p>2 options `userKey` and `pwdKey` permits indicating which environment variable to use.
 */
public class Env2CredentialPlugin implements CredentialPlugin {

  @Override
  public String type() {
    return "ENVTEST";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  @Override
  public String defaultAuthenticationPluginType() {
    return "mysql_native_password";
  }

  @Override
  public Credential get() {
    return new Credential(System.getenv("MARIADB2_USER"), System.getenv("MARIADB2_PWD"));
  }
}
