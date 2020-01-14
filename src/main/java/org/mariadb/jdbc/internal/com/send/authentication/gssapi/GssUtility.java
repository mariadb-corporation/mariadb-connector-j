package org.mariadb.jdbc.internal.com.send.authentication.gssapi;

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
