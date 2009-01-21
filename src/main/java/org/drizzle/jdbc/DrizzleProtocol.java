package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.*;

import javax.net.SocketFactory;
import java.net.Socket;
import java.io.*;

/**
 * TODO: logging!
 * TODO: refactor, clean up
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 4:06:26 PM
 */
public class DrizzleProtocol implements Protocol {
    private byte packetSeqNum = 1;
    private boolean connected=false;
    private Socket socket;
    private BufferedInputStream reader;
    private BufferedOutputStream writer;
    public DrizzleProtocol(String host, int port, String username, String password) throws IOException, UnauthorizedException {
        SocketFactory socketFactory = SocketFactory.getDefault();
        socket = socketFactory.createSocket(host,port);
        reader = new BufferedInputStream(socket.getInputStream(),16384);
        writer = new BufferedOutputStream(socket.getOutputStream(),16384);
        this.connect(username,password);
    }

    public void connect(String username, String password) throws UnauthorizedException, IOException {
        this.connected=true;
        GreetingReadPacket greetingPacket = new GreetingReadPacket(reader);
        ClientAuthPacket cap = new ClientAuthPacket(greetingPacket.getServerCapabilities(), greetingPacket.getServerLanguage());
        byte [] a = cap.toBytes(packetSeqNum);
        writer.write(a);
        writer.flush();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(reader);
        packetSeqNum=(byte)(resultPacket.getPacketSeq()+1);
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
        packetSeqNum=0; 
        byte [] toWrite = packet.toBytes(packetSeqNum);
        writer.write(toWrite);
        writer.flush();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(reader);
        System.out.println(resultPacket.getResultType());
        System.out.println(((ResultSetPacket)resultPacket).getFieldCount());
        packetSeqNum=(byte)(resultPacket.getPacketSeq()+1);
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void selectDB(String database) throws IOException {
        SelectDBPacket packet = new SelectDBPacket(database);
        packetSeqNum=0;
        byte [] b = packet.getBytes(packetSeqNum);
        writer.write(b);
        writer.flush();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(reader);
        packetSeqNum=(byte)(resultPacket.getPacketSeq()+1);
    }

    public void clearInputStream() throws IOException {
        if(reader.available() > 0) {
            byte [] aa = new byte[reader.available()];
            reader.read(aa);
        }
    }

}
