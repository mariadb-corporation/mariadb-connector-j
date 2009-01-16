package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.GreetingReadPacket;
import org.drizzle.jdbc.packet.ClientAuthPacket;

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
    /*    byte [] a = new ClientAuthPacket().toByteArrayWithLength();
        for(byte aB : a)System.out.printf("0x%x\n",aB);
        socket.getOutputStream().write(a);
        socket.getOutputStream().flush();
        byte[]aaa = new byte[1000];
        socket.getInputStream().read(aaa);
        for(byte aB : aaa) System.out.printf("0x%x\n",aB);*/
        //while(true);
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
