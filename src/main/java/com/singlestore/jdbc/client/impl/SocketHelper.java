package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.Configuration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketHelper {
  public static void setSocketOption(final Configuration conf, final Socket socket)
      throws IOException {
    socket.setTcpNoDelay(true);
    socket.setSoTimeout(conf.socketTimeout());
    if (conf.tcpKeepAlive()) {
      socket.setKeepAlive(true);
    }
    if (conf.tcpAbortiveClose()) {
      socket.setSoLinger(true, 0);
    }

    // Bind the socket to a particular interface if the connection property
    // localSocketAddress has been defined.
    if (conf.localSocketAddress() != null) {
      InetSocketAddress localAddress = new InetSocketAddress(conf.localSocketAddress(), 0);
      socket.bind(localAddress);
    }
  }
}
