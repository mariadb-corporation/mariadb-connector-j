// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import java.sql.SQLException;

public interface CredentialPlugin {

  String type();

  Credential get() throws SQLException;

  default boolean mustUseSsl() {
    return false;
  }

  default String defaultAuthenticationPluginType() {
    return null;
  }

  default CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress)
      throws SQLException {
    return this;
  }
}
