// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin;

import java.io.IOException;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
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
   * Build an {@link SSLSocketFactory} for the given configuration.
   *
   * <p>A fresh {@code SSLContext} is built on every call, so its (single-use) client SSL session
   * cache is never shared between connections and TLS session resumption cannot occur across
   * connections. The trust/key managers, the comparatively expensive part (loading key/trust
   * stores), are what implementations are expected to cache, not the factory itself.
   *
   * @param conf configuration
   * @param exceptionFactory exception factory
   * @param hostAddress host address (used to resolve a per-host sslMode override)
   * @return SSL socket factory
   * @throws SQLException if the SSL context cannot be initialized
   */
  default SSLSocketFactory getSocketFactory(
      Configuration conf, ExceptionFactory exceptionFactory, HostAddress hostAddress)
      throws SQLException {
    return newSslSocketFactory(
        getKeyManager(conf, exceptionFactory),
        getTrustManager(conf, exceptionFactory, hostAddress),
        exceptionFactory);
  }

  /**
   * Build a fresh {@link SSLSocketFactory} from the given managers. A new {@code SSLContext} is
   * created on each call, so no client SSL session cache is shared between connections (no TLS
   * session resumption across connections).
   *
   * @param keyManagers key managers (maybe {@code null})
   * @param trustManagers trust managers
   * @param exceptionFactory exception factory
   * @return a new SSL socket factory
   * @throws SQLException if the SSL context cannot be initialized
   */
  static SSLSocketFactory newSslSocketFactory(
      KeyManager[] keyManagers, TrustManager[] trustManagers, ExceptionFactory exceptionFactory)
      throws SQLException {
    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyManagers, trustManagers, null);
      return sslContext.getSocketFactory();
    } catch (KeyManagementException e) {
      throw exceptionFactory.create("Could not initialize SSL context", "08000", e);
    } catch (NoSuchAlgorithmException e) {
      throw exceptionFactory.create("SSLContext TLS Algorithm not unknown", "08000", e);
    }
  }

  /**
   * Returns a socket layered over an existing socket negotiating the use of SSL over an existing
   * socket.
   *
   * <p>The peer host is the name the connection was made with (without trailing dot): JSSE uses it
   * for SNI and, with {@code sslMode=verify-full}, for the standard endpoint identification
   * (hostname verification) of the server certificate during the handshake.
   *
   * @param socket existing socket
   * @param sslSocketFactory SSL socket factory
   * @param peerHost host name of the server, or null when unknown (unix socket, pipe)
   * @return SSL socket
   * @throws IOException if any socket error occurs.
   */
  default SSLSocket createSocket(Socket socket, SSLSocketFactory sslSocketFactory, String peerHost)
      throws IOException {
    return (SSLSocket) sslSocketFactory.createSocket(socket, peerHost, socket.getPort(), true);
  }
}
