package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.GreetingReadPacket;
import org.drizzle.jdbc.packet.ClientAuthPacket;
import org.drizzle.jdbc.packet.ResultPacketFactory;
import org.drizzle.jdbc.packet.ResultPacket;

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
    private long serverThreadID;
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
        GreetingReadPacket greetingPacket = new GreetingReadPacket(reader);
        ClientAuthPacket cap = new ClientAuthPacket(greetingPacket.getServerCapabilities(), greetingPacket.getServerLanguage());
        byte [] a = cap.toBytes();
        socket.getOutputStream().write(a);
        socket.getOutputStream().flush();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(socket.getInputStream());
        System.out.println(resultPacket.getResultType());
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

    public DrizzleRows executeQuery(String s) throws IOException {
        QueryPacket packet = new QueryPacket(s);
        byte [] toWrite = packet.toBytes();
        socket.getOutputStream().write(toWrite);
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
