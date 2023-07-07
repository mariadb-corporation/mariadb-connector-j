// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.env;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.Credential;
import com.singlestore.jdbc.plugin.CredentialPlugin;

/**
 * Authentication using environment variable.
 *
 * <p>default implementation use environment variable SINGLESTORE_USER and SINGLESTORE_PWD
 *
 * <p>example : `jdbc:singlestore://host/db?credentialType=ENV`
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which environment variable to use.
 */
public class EnvCredentialPlugin implements CredentialPlugin {

  private Configuration conf;
  private String userName;

  @Override
  public String type() {
    return "ENV";
  }

  @Override
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress) {
    this.conf = conf;
    this.userName = userName;
    return this;
  }

  @Override
  public Credential get() {

    String userKey = this.conf.nonMappedOptions().getProperty("userKey");
    String pwdKey = this.conf.nonMappedOptions().getProperty("pwdKey");
    String envUser = System.getenv(userKey != null ? userKey : "SINGLESTORE_USER");
    return new Credential(
        envUser == null ? userName : envUser,
        System.getenv(pwdKey != null ? pwdKey : "SINGLESTORE_PWD"));
  }
}
