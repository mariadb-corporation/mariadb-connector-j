// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Credential {
  private final String password;
  private final String user;

  /**
   * Constructor of basic credential
   *
   * @param user user
   * @param password password
   */
  public Credential(@JsonProperty("user") String user, @JsonProperty("password") String password) {
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
