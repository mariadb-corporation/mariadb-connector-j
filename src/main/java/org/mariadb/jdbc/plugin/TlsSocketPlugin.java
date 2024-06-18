// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import javax.net.ssl.*;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.export.ExceptionFactory;

/** TLS Socket interface plugin */
public interface TlsSocketPlugin {

  /**
   * plugin type.
   *
   * @return plugin type
   */
  String type();

  TrustManager[] getTrustManager(
      Configuration conf, ExceptionFactory exceptionFactory, HostAddress hostAddress)
      throws SQLException;

  KeyManager[] getKeyManager(Configuration conf, ExceptionFactory exceptionFactory)
      throws SQLException;

  /**
   * Returns a socket layered over an existing socket negotiating the use of SSL over an existing
   * socket.
   *
   * @param socket existing socket
   * @param sslSocketFactory SSL socket factory
   * @return SSL socket
   * @throws IOException if any socket error occurs.
   */
  default SSLSocket createSocket(Socket socket, SSLSocketFactory sslSocketFactory)
      throws IOException {
    return (SSLSocket)
        sslSocketFactory.createSocket(
            socket,
            socket.getInetAddress() == null ? null : socket.getInetAddress().getHostAddress(),
            socket.getPort(),
            true);
  }

  /**
   * Host name verifier implementation.
   *
   * @param host hostname
   * @param sslSession ssl session
   * @param serverThreadId current server threadId
   * @throws SSLException if verification fail
   */
  void verify(String host, SSLSession sslSession, long serverThreadId) throws SSLException;
}
