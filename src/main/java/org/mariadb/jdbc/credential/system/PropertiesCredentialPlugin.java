/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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

package org.mariadb.jdbc.credential.system;

import org.mariadb.jdbc.*;
import org.mariadb.jdbc.credential.*;
import org.mariadb.jdbc.util.*;

/**
 * Authentication using java system properties.
 *
 * <p>default implementation use system properties `mariadb.user` and `mariadb.pwd`
 *
 * <p>example : `jdbc:mariadb://host/db?identityType=PROPERTY` will use environment variable
 * MARIADB_USER and MARIADB_PWD
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which environment variable to use.
 */
/**
 * Authentication using java system properties.
 *
 * <p>default implementation use system properties `mariadb.user` and `mariadb.pwd`
 *
 * <p>example : `jdbc:mariadb://host/db?identityType=PROPERTY`
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which system properties to use .
 */
public class PropertiesCredentialPlugin implements CredentialPlugin {

  private Options options;
  private String userName;

  @Override
  public String type() {
    return "PROPERTY";
  }

  @Override
  public String name() {
    return "Environment password";
  }

  @Override
  public CredentialPlugin initialize(Options options, String userName, HostAddress hostAddress) {
    this.options = options;
    this.userName = userName;
    return this;
  }

  @Override
  public Credential get() {

    String userKey = this.options.nonMappedOptions.getProperty("userKey");
    String pwdKey = this.options.nonMappedOptions.getProperty("pwdKey");
    String propUser = System.getProperty(userKey != null ? userKey : "mariadb.user");
    return new Credential(
        propUser == null ? userName : propUser,
        System.getProperty(pwdKey != null ? pwdKey : "mariadb.pwd"));
  }
}
