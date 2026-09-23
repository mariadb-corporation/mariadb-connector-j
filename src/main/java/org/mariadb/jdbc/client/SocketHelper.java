// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import jdk.net.ExtendedSocketOptions;
import org.mariadb.jdbc.Configuration;

/** Socket option helper */
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

    if (conf.tcpKeepIdle() > 0) {
      socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, conf.tcpKeepIdle());
    }
    if (conf.tcpKeepCount() > 0) {
      socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, conf.tcpKeepCount());
    }
    if (conf.tcpKeepInterval() > 0) {
      socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, conf.tcpKeepInterval());
    }

    // Bind the socket to a particular interface if the connection property
    // localSocketAddress has been defined.
    if (conf.localSocketAddress() != null) {
      InetSocketAddress localAddress = new InetSocketAddress(conf.localSocketAddress(), 0);
      socket.bind(localAddress);
    }
  }
}
