package org.mariadb.jdbc.internal.io.socket;

import org.mariadb.jdbc.util.*;

import java.io.*;
import java.net.*;

@FunctionalInterface
public interface SocketHandlerFunction {

  Socket apply(Options options, String host) throws IOException;
}
