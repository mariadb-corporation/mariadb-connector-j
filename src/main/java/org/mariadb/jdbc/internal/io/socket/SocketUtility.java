package org.mariadb.jdbc.internal.io.socket;

import com.sun.jna.Platform;
import java.io.IOException;
import org.mariadb.jdbc.internal.util.Utils;

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
      //forcing use of JNA to ensure AOT compilation
      Platform.getOSType();

      return (options, host) -> {
        if (options.pipe != null) {
          return new NamedPipeSocket(host, options.pipe);
        } else if (options.localSocket != null) {
          try {
            return new UnixDomainSocket(options.localSocket);
          } catch (RuntimeException re) {
            throw new IOException(re.getMessage(), re.getCause());
          }
        } else if (options.sharedMemory != null) {
          try {
            return new SharedMemorySocket(options.sharedMemory);
          } catch (RuntimeException re) {
            throw new IOException(re.getMessage(), re.getCause());
          }
        } else {
          return Utils.standardSocket(options, host);
        }

      };
    } catch (Throwable cle) {
      //jna jar's are not in classpath
    }
    return (options, host) -> Utils.standardSocket(options, host);
  }
}
