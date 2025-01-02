// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.server.util;

import org.mariadb.jdbc.client.ServerVersion;
import org.mariadb.jdbc.util.Version;

/** Server version utility */
public final class ServerVersionUtility extends Version implements ServerVersion {

  private final boolean mariaDBServer;

  /**
   * Constructor
   *
   * @param serverVersion server version string
   * @param mariaDBServer is server mariadb
   */
  public ServerVersionUtility(String serverVersion, boolean mariaDBServer) {
    super(serverVersion);
    this.mariaDBServer = mariaDBServer;
  }

  /**
   * Is server mariadb
   *
   * @return true if server is a MariaDB server
   */
  public boolean isMariaDBServer() {
    return mariaDBServer;
  }
}
