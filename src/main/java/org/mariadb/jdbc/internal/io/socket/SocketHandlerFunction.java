package org.mariadb.jdbc.internal.io.socket;

import org.mariadb.jdbc.UrlParser;

import java.io.IOException;
import java.net.Socket;

@FunctionalInterface
public interface SocketHandlerFunction {
    Socket apply(UrlParser a, String b) throws IOException;
}