// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.socket.impl;

import com.sun.jna.Platform;
import java.io.IOException;
import org.mariadb.jdbc.client.impl.ConnectionHelper;

/**
 * Socket Utility, to defined function that will create socket according to dependency and
 * configuration
 */
public class SocketUtility {

  /**
   * Create socket according to options. In case of compilation ahead of time, will throw an error
   * if dependencies found, then use default socket implementation.
   *
   * @return Socket
   */
  @SuppressWarnings("unchecked")
  public static SocketHandlerFunction getSocketHandler() {
    // forcing use of JNA to ensure AOT compilation
    Platform.getOSType();

    return (conf, hostAddress) -> {
      if (conf.pipe() != null) {
        return new NamedPipeSocket(hostAddress != null ? hostAddress.host : null, conf.pipe());
      } else if (conf.localSocket() != null) {
        try {
          return new UnixDomainSocket(conf.localSocket());
        } catch (RuntimeException re) {
          throw new IOException(re.getMessage(), re.getCause());
        }
      } else {
        return ConnectionHelper.standardSocket(conf, hostAddress);
      }
    };
  }
}
