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

package org.mariadb.jdbc.internal.com.send.authentication.gssapi;

import com.sun.jna.Platform;
import org.mariadb.jdbc.plugin.authentication.standard.gssapi.GssapiAuth;
import org.mariadb.jdbc.plugin.authentication.standard.gssapi.StandardGssapiAuthentication;
import org.mariadb.jdbc.plugin.authentication.standard.gssapi.WindowsNativeSspiAuthentication;

public class GssUtility {

  /**
   * Get authentication method according to classpath. Windows native authentication is using
   * Waffle-jna.
   *
   * @return authentication method
   */
  public static GssapiAuth getAuthenticationMethod() {
    try {
      // Waffle-jna has jna as dependency, so if not available on classpath, just use standard
      // authentication
      if (Platform.isWindows()) {
        try {
          Class.forName("waffle.windows.auth.impl.WindowsAuthProviderImpl");
          return new WindowsNativeSspiAuthentication();
        } catch (ClassNotFoundException cle) {
          // waffle not in the classpath
        }
      }
    } catch (Throwable cle) {
      // jna jar's are not in classpath
    }
    return new StandardGssapiAuthentication();
  }
}
