/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.client.socket;

import com.sun.jna.Platform;
import java.io.IOException;
import org.mariadb.jdbc.client.ConnectionHelper;

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

      return (conf, host) -> {
        if (conf.pipe() != null) {
          return new NamedPipeSocket(host, conf.pipe());
        } else if (conf.localSocket() != null) {
          try {
            return new UnixDomainSocket(conf.localSocket());
          } catch (RuntimeException re) {
            throw new IOException(re.getMessage(), re.getCause());
          }
        } else {
          return ConnectionHelper.standardSocket(conf, host);
        }
      };
    } catch (Throwable cle) {
      // jna jar's are not in classpath
    }
    return ConnectionHelper::standardSocket;
  }
}
