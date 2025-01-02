// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.mariadb.jdbc.Configuration;

public class SocketHelper {

  /**
   * Set socket option
   *
   * @param conf configuration
   * @param socket socket
   * @throws IOException if any socket error occurs
   */
  public static void setSocketOption(final Configuration conf, final Socket socket)
      throws IOException {
    socket.setTcpNoDelay(true);
    socket.setSoTimeout(conf.socketTimeout());
    if (conf.tcpKeepAlive()) {
      socket.setKeepAlive(true);
    }
    if (conf.tcpAbortiveClose()) {
      socket.setSoLinger(true, 0);
    }

    // Bind the socket to a particular interface if the connection property
    // localSocketAddress has been defined.
    if (conf.localSocketAddress() != null) {
      InetSocketAddress localAddress = new InetSocketAddress(conf.localSocketAddress(), 0);
      socket.bind(localAddress);
    }
  }
}
