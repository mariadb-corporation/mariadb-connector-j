// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin;

/** basic Credential (couple of user/password) */
public class Credential {
  private final String password;
  private final String user;

  /**
   * Constructor of basic credential
   *
   * @param user user
   * @param password password
   */
  public Credential(String user, String password) {
    this.user = user;
    this.password = password;
  }

  /**
   * Get user
   *
   * @return user
   */
  public String getUser() {
    return user;
  }

  /**
   * Get password
   *
   * @return password
   */
  public String getPassword() {
    return password;
  }
}
