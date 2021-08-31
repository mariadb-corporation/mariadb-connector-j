// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server.util;

import org.mariadb.jdbc.client.ServerVersion;
import org.mariadb.jdbc.util.Version;

public final class ServerVersionUtility extends Version implements ServerVersion {

  private final boolean mariaDBServer;

  public ServerVersionUtility(String serverVersion, boolean mariaDBServer) {
    super(serverVersion);
    this.mariaDBServer = mariaDBServer;
  }

  public boolean isMariaDBServer() {
    return mariaDBServer;
  }
}
