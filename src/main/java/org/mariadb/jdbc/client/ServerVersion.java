// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

import org.mariadb.jdbc.util.Version;

public final class ServerVersion extends Version {

  private final boolean mariaDBServer;

  public ServerVersion(String serverVersion, boolean mariaDBServer) {
    super(serverVersion);
    this.mariaDBServer = mariaDBServer;
  }

  public boolean isMariaDBServer() {
    return mariaDBServer;
  }
}
