// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.util;

import com.singlestore.jdbc.plugin.credential.Credential;
import com.singlestore.jdbc.plugin.credential.CredentialPlugin;

/**
 * Authentication using environment variable.
 *
 * <p>default implementation use environment variable SINGLESTORE_USER and SINGLESTORE_PWD
 *
 * <p>example : `jdbc:singlestore://host/db?credentialType=ENV`
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which environment variable to use.
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
    return new Credential(System.getenv("SINGLESTORE2_USER"), System.getenv("SINGLESTORE2_PWD"));
  }
}
