// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.socket;

import com.singlestore.jdbc.client.ConnectionHelper;
import com.sun.jna.Platform;
import java.io.IOException;

public class SocketUtility {

  /**
   * Create socket according to options. In case of compilation ahead of time, will throw an error
   * if dependencies found, then use default socket implementation.
   *
   * @return Socket
   */
  @SuppressWarnings("unchecked")
  public static SocketHandlerFunction getSocketHandler() {
    try {
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
    } catch (Throwable cle) {
      // jna jar's are not in classpath
    }
    return ConnectionHelper::standardSocket;
  }
}
