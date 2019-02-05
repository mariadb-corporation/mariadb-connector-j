package org.mariadb.jdbc.internal.io.socket;

import com.sun.jna.Platform;
import java.io.IOException;
import java.net.Socket;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.util.Utils;

public class SocketUtility {

  /**
   * Create socket according to options.
   * In case of compilation ahead of time, will throw an error if dependencies found,
   * then use default socket implementation.
   *
   * @param urlParser connection string parser info
   * @param host      host
   * @return Socket
   * @throws IOException if socket fail to create
   */
  @SuppressWarnings("unchecked")
  public static Socket getSocketHandler(UrlParser urlParser, String host) throws IOException {
    try {
      //forcing use of JNA to ensure AOT compilation
      Platform.getOSType();

      if (urlParser.getOptions().pipe != null) {
        return new NamedPipeSocket(host, urlParser.getOptions().pipe);
      } else if (urlParser.getOptions().localSocket != null) {
        try {
          return new UnixDomainSocket(urlParser.getOptions().localSocket);
        } catch (RuntimeException re) {
          throw new IOException(re.getMessage(), re.getCause());
        }
      } else if (urlParser.getOptions().sharedMemory != null) {
        try {
          return new SharedMemorySocket(urlParser.getOptions().sharedMemory);
        } catch (RuntimeException re) {
          throw new IOException(re.getMessage(), re.getCause());
        }
      } else {
        return Utils.standardSocket(urlParser, host);
      }

    } catch (Throwable cle) {
      //jna jar's are not in classpath
    }
    return Utils.standardSocket(urlParser, host);
  }
}
