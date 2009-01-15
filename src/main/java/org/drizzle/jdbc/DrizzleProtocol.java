package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.GreetingPacket;

import javax.net.SocketFactory;
import java.net.Socket;
import java.io.*;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 4:06:26 PM
 */
public class DrizzleProtocol implements Protocol {
    private boolean connected=false;
    private Socket socket;
    private PrintWriter writer;
    private InputStream reader;
    private int serverThreadID;
    private String serverVersion;
    private int protocolVersion;
    public DrizzleProtocol(String host, int port, String username, String password) throws IOException, UnauthorizedException {
        SocketFactory socketFactory = SocketFactory.getDefault();
        socket = socketFactory.createSocket(host,port);
        this.connect(username,password);
    }

    public void connect(String username, String password) throws UnauthorizedException, IOException {
        // talk to drizzle
        // validate uname and password
        this.connected=true;
        this.writer=new PrintWriter(socket.getOutputStream());
        this.reader = socket.getInputStream();
        GreetingPacket greetingPacket = new GreetingPacket(reader);
        serverVersion = greetingPacket.getServerVersion();
        serverThreadID = greetingPacket.getServerThreadID();
        protocolVersion = greetingPacket.getProtocolVersion();
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
        this.connected=false;
    }

    public boolean isClosed() {
        return !this.connected;
    }

}
