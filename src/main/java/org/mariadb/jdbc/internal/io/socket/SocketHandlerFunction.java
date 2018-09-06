package org.mariadb.jdbc.internal.io.socket;

import java.io.IOException;
import java.net.Socket;
import org.mariadb.jdbc.UrlParser;

@FunctionalInterface
public interface SocketHandlerFunction {

  Socket apply(UrlParser urlParser, String host) throws IOException;
}