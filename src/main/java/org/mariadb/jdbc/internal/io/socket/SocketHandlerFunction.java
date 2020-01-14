package org.mariadb.jdbc.internal.io.socket;

import org.mariadb.jdbc.util.Options;

import java.io.IOException;
import java.net.Socket;

@FunctionalInterface
public interface SocketHandlerFunction {

  Socket apply(Options options, String host) throws IOException;
}
