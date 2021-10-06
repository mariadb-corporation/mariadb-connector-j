// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin.tls;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.util.exceptions.ExceptionFactory;
import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

public interface TlsSocketPlugin {

  /**
   * plugin type.
   *
   * @return plugin type
   */
  String type();

  /**
   * Get socket factory.
   *
   * @param conf connection string option. Non standard option are stored in `nonMappedOptions` if
   *     any specific option is needed.
   * @param exceptionFactory exception handler
   * @return custom SSL socket factory
   * @throws SQLException if socket factory configuration failed.
   */
  SSLSocketFactory getSocketFactory(Configuration conf, ExceptionFactory exceptionFactory)
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
