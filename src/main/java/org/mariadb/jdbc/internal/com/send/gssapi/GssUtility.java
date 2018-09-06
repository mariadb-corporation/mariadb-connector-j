package org.mariadb.jdbc.internal.com.send.gssapi;

import com.sun.jna.Platform;
import java.util.function.BiFunction;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;

public class GssUtility {

  /**
   * Get authentication method according to classpath. Windows native authentication is using
   * Waffle-jna.
   *
   * @return authentication method
   */
  public static BiFunction<PacketInputStream, Integer, GssapiAuth> getAuthenticationMethod() {
    try {
      //Waffle-jna has jna as dependency, so if not available on classpath, just use standard authentication
      if (Platform.isWindows()) {
        try {
          Class.forName("waffle.windows.auth.impl.WindowsAuthProviderImpl");
          return (reader, packSeq) -> new WindowsNativeSspiAuthentication(reader, packSeq);
        } catch (ClassNotFoundException cle) {
          //waffle not in the classpath
        }
      }
    } catch (Throwable cle) {
      //jna jar's are not in classpath
    }
    return (reader, packSeq) -> new StandardGssapiAuthentication(reader, packSeq);
  }

}
