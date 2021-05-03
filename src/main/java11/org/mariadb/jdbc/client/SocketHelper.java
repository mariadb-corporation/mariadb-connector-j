package org.mariadb.jdbc.client;

import jdk.net.ExtendedSocketOptions;
import org.mariadb.jdbc.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketHelper {
    public static void setSocketOption(final Configuration conf, final Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(conf.socketTimeout());
        if (conf.tcpKeepAlive()) {
            socket.setKeepAlive(true);
        }
        if (conf.tcpAbortiveClose()) {
            socket.setSoLinger(true, 0);
        }

        // java 11 only
        if (conf.tcpKeepIdle() > 0) {
            socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, conf.tcpKeepIdle());
        }
        if (conf.tcpKeepCount() > 0) {
            socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, conf.tcpKeepCount());
        }
        if (conf.tcpKeepInterval() > 0) {
            socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, conf.tcpKeepInterval());
        }

        // Bind the socket to a particular interface if the connection property
        // localSocketAddress has been defined.
        if (conf.localSocketAddress() != null) {
            InetSocketAddress localAddress = new InetSocketAddress(conf.localSocketAddress(), 0);
            socket.bind(localAddress);
        }
    }
}
