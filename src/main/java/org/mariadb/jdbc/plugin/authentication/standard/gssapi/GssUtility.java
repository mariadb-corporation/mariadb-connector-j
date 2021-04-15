// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.authentication.standard.gssapi;

import com.sun.jna.Platform;

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
