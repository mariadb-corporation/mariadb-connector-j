/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.integration.util;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.plugin.credential.Credential;
import org.mariadb.jdbc.plugin.credential.CredentialPlugin;

/**
 * Authentication using environment variable.
 *
 * <p>default implementation use environment variable MARIADB_USER and MARIADB_PWD
 *
 * <p>example : `jdbc:mariadb://host/db?credentialType=ENV`
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which environment variable to use.
 */
public class Env2CredentialPlugin implements CredentialPlugin {

  private Configuration conf;
  private String userName;

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
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress) {
    this.conf = conf;
    this.userName = userName;
    return this;
  }

  @Override
  public Credential get() {
    return new Credential(System.getenv("MARIADB_USER"), System.getenv("MARIADB_PWD"));
  }
}
