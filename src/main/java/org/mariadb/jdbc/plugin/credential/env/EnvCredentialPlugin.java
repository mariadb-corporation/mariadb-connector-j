// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.credential.env;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
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
    String envUser = System.getenv(userKey != null ? userKey : "MARIADB_USER");
    return new Credential(
        envUser == null ? userName : envUser,
        System.getenv(pwdKey != null ? pwdKey : "MARIADB_PWD"));
  }
}
