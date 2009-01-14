package org.drizzle.jdbc;

import javax.net.SocketFactory;
import java.net.Socket;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 4:06:26 PM
 */
public class DrizzleProtocol implements Protocol {
    private boolean connected=false;
    private Socket socket;
    public DrizzleProtocol(String host, int port, String username, String password) throws IOException, UnauthorizedException {
        SocketFactory socketFactory = SocketFactory.getDefault();
        socket = socketFactory.createSocket(host,port);
        this.connect(username,password);
    }

    public void connect(String username, String password) throws UnauthorizedException {
        // talk to drizzle
        // validate uname and password
        this.connected=true;
    }

    public void close() throws IOException {
        socket.close();
        this.connected=false;
    }

    public boolean isClosed() {
        return !this.connected;
    }

}
