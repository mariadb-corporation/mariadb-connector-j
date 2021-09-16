// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.plugin.credential.system;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.credential.Credential;
import com.singlestore.jdbc.plugin.credential.CredentialPlugin;

/**
 * Authentication using java system properties.
 *
 * <p>default implementation use system properties `mariadb.user` and `mariadb.pwd`
 *
 * <p>example : `jdbc:singlestore://host/db?identityType=PROPERTY` will use environment variable
 * MARIADB_USER and MARIADB_PWD
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which environment variable to use.
 */
/**
 * Authentication using java system properties.
 *
 * <p>default implementation use system properties `mariadb.user` and `mariadb.pwd`
 *
 * <p>example : `jdbc:singlestore://host/db?credentialType=PROPERTY`
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which system properties to use .
 */
public class PropertiesCredentialPlugin implements CredentialPlugin {

  private Configuration conf;
  private String userName;

  @Override
  public String type() {
    return "PROPERTY";
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
    String propUser = System.getProperty(userKey != null ? userKey : "mariadb.user");

    return new Credential(
        propUser == null ? userName : propUser,
        System.getProperty(pwdKey != null ? pwdKey : "mariadb.pwd"));
  }
}
