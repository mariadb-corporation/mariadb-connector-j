package org.mariadb.jdbc.internal.io.socket;

import java.io.IOException;
import java.net.Socket;
import org.mariadb.jdbc.util.Options;

@FunctionalInterface
public interface SocketHandlerFunction {

  Socket apply(Options options, String host) throws IOException;
}