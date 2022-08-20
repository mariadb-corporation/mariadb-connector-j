// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.client.socket.impl;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import org.tidb.jdbc.Configuration;
import org.tidb.jdbc.HostAddress;

/** Construct socket depending on configuration helper */
@FunctionalInterface
public interface SocketHandlerFunction {
  /**
   * Create socket
   *
   * @param conf configuration
   * @param hostAddress host
   * @return socket
   * @throws IOException if any socket issue occurs
   * @throws SQLException for other kind of error
   */
  Socket apply(Configuration conf, HostAddress hostAddress) throws IOException, SQLException;
}
