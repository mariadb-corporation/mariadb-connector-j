package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.*;
import org.drizzle.jdbc.packet.buffer.ReadBuffer;

import javax.net.SocketFactory;
import java.net.Socket;
import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

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
    public DrizzleProtocol(String host, int port, String database, String username, String password) throws IOException, UnauthorizedException {
        SocketFactory socketFactory = SocketFactory.getDefault();
        socket = socketFactory.createSocket(host,port);
        reader = new BufferedInputStream(socket.getInputStream(),16384);
        writer = new BufferedOutputStream(socket.getOutputStream(),16384);
        //System.out.println("db============="+database);
        this.connect(username,password,database);

    }

    public void connect(String username, String password, String database) throws UnauthorizedException, IOException {
        this.connected=true;
        GreetingReadPacket greetingPacket = new GreetingReadPacket(reader);
        ClientAuthPacket cap = new ClientAuthPacket(username,password,database);
        cap.setServerCapabilities(greetingPacket.getServerCapabilities());
        cap.setServerLanguage(greetingPacket.getServerLanguage());
        byte [] a = cap.toBytes(packetSeqNum);
        writer.write(a);
        writer.flush();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(reader);
        packetSeqNum=(byte)(resultPacket.getPacketSeq()+1);
        selectDB(database);
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

    public DrizzleQueryResult executeQuery(String s) throws IOException, SQLException {
        QueryPacket packet = new QueryPacket(s);
        packetSeqNum=0; 
        byte [] toWrite = packet.toBytes(packetSeqNum);
        writer.write(toWrite);
        writer.flush();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(reader);
        packetSeqNum=(byte)(resultPacket.getPacketSeq()+1);
        //System.out.println(resultPacket.getResultType());
        switch(resultPacket.getResultType()) {
            case ERROR:
                throw new SQLException("Could not execute query: "+((ErrorPacket)resultPacket).getMessage());
            case OK:
                return DrizzleQueryResult.OKRESULT;
            case RESULTSET:
                return this.createDrizzleQueryResult((ResultSetPacket)resultPacket);
            default:
                throw new SQLException("Could not parse result");
        }
    }
    private DrizzleQueryResult createDrizzleQueryResult(ResultSetPacket packet) throws IOException {
        //System.out.println("creating drizzle qr");
        List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();
        for(int i=0;i<packet.getFieldCount();i++) {
            FieldPacket fieldPacket = new FieldPacket(new ReadBuffer(reader));
            //System.out.println(fieldPacket);
            fieldPackets.add(fieldPacket);
        }
        ReadBuffer readBuffer = new ReadBuffer(reader);
        if( (readBuffer.getByteAt(0)==-2) && (readBuffer.getLength()<9)) { //check for EOF
            //System.out.println("found EOF packet");
        } else {
            throw new IOException("Could not parse result");
        }
        DrizzleQueryResult dqr = new DrizzleQueryResult(fieldPackets);
        while(true) {
            readBuffer = new ReadBuffer(reader);
            if((readBuffer.getByteAt(0)==-2) && (readBuffer.getLength()<9)) { //check for EOF
                return dqr;
            }
            RowPacket rowPacket = new RowPacket(readBuffer,packet.getFieldCount());
            dqr.addRow(rowPacket.getRow());
        }
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
