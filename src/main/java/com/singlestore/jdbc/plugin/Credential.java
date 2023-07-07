// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Credential {
  private final String password;
  private final String user;

  public Credential(@JsonProperty("user") String user, @JsonProperty("password") String password) {
    this.user = user;
    this.password = password;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }
}
