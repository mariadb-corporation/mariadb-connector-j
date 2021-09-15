// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.plugin.credential;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import java.sql.SQLException;
import java.util.function.Supplier;

public interface CredentialPlugin extends Supplier<Credential> {

  String type();

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
