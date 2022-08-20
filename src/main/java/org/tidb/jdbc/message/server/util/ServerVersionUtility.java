// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.message.server.util;

import org.tidb.jdbc.client.ServerVersion;
import org.tidb.jdbc.util.Version;

/** Server version utility */
public final class ServerVersionUtility extends Version implements ServerVersion {

  private final boolean tidbServer;

  /**
   * Constructor
   *
   * @param serverVersion server version string
   * @param tidbServer is tidb server
   */
  public ServerVersionUtility(String serverVersion, boolean tidbServer) {
    super(serverVersion);
    this.tidbServer = tidbServer;
  }

  /**
   * Is tidb server
   *
   * @return true if server is a MariaDB server
   */
  public boolean isTiDBServer() {
    return tidbServer;
  }
}
