// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.token;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.Credential;
import com.singlestore.jdbc.plugin.CredentialPlugin;

/**
 * Authentication using JWT
 *
 * <p>The token is sent via Cleartext plugin, so SSL has to be enabled
 */
public class JwtCredentialPlugin implements CredentialPlugin {
  private String userName;
  private String token;

  @Override
  public String type() {
    return "JWT";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  @Override
  public String defaultAuthenticationPluginType() {
    return "mysql_clear_password";
  }

  @Override
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress) {
    this.userName = userName;
    this.token = conf.password();
    return this;
  }

  @Override
  public Credential get() {
    return new Credential(userName, token);
  }
}
