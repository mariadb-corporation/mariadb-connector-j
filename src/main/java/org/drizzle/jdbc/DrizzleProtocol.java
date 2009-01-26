package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.*;
import org.drizzle.jdbc.packet.buffer.ReadBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.net.Socket;
import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

/**
 * TODO: logging!
 * TODO: refactor, clean up
 * TODO: when should i read up the resultset?
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 4:06:26 PM
 */
public class DrizzleProtocol implements Protocol {
    private static Logger log = LoggerFactory.getLogger(DrizzleProtocol.class);
    private byte packetSeqNum = 1;
    private boolean connected=false;
    private Socket socket;
    private BufferedInputStream reader;
    private BufferedOutputStream writer;

    public DrizzleProtocol(String host, int port, String database, String username, String password) throws IOException, UnauthorizedException {
        SocketFactory socketFactory = SocketFactory.getDefault();
        socket = socketFactory.createSocket(host,port);
        log.info("Connected to: {}:{}",host,port);
        reader = new BufferedInputStream(socket.getInputStream(),16384);
        writer = new BufferedOutputStream(socket.getOutputStream(),16384);
        this.connect(username,password,database);

    }

    public void connect(String username, String password, String database) throws UnauthorizedException, IOException {
        this.connected=true;
        GreetingReadPacket greetingPacket = new GreetingReadPacket(reader);
        log.debug("Got greeting packet: {}",greetingPacket);
        ClientAuthPacket cap = new ClientAuthPacket(username,password,database);
        cap.setServerCapabilities(greetingPacket.getServerCapabilities());
        cap.setServerLanguage(greetingPacket.getServerLanguage());
        byte [] a = cap.toBytes(packetSeqNum);
        writer.write(a);
        writer.flush();
        log.debug("Sending auth packet: {}",cap);
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(reader);
        log.debug("Got result: {}",resultPacket);
        packetSeqNum=(byte)(resultPacket.getPacketSeq()+1);
        selectDB(database);
    }

    public void close() throws IOException {
        log.debug("Closing...");
        writer.close();
        reader.close();
        socket.close();
        this.connected=false;
    }

    public boolean isClosed() {
        return !this.connected;
    }

    public DrizzleQueryResult executeQuery(String s) throws IOException, SQLException {
        log.debug("Executing query: {}",s);
        QueryPacket packet = new QueryPacket(s);
        packetSeqNum=0; 
        byte [] toWrite = packet.toBytes(packetSeqNum);
        writer.write(toWrite);
        writer.flush();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(reader);
        packetSeqNum=(byte)(resultPacket.getPacketSeq()+1);
        switch(resultPacket.getResultType()) {
            case ERROR:
                log.warn("Could not execute query: {}",((ErrorPacket)resultPacket).getMessage());
                throw new SQLException("Could not execute query: "+((ErrorPacket)resultPacket).getMessage());
            case OK:
                log.info("OK, {}", ((OKPacket)resultPacket).getAffectedRows());
                //TODO: yeah, pass this info to client somehow...
                return new DrizzleQueryResult();
            case RESULTSET:
                log.info("SELECT executed, fetching result set");
                return this.createDrizzleQueryResult((ResultSetPacket)resultPacket);
            default:
                log.error("Could not parse result...");
                throw new SQLException("Could not parse result");
        }
    }
    private DrizzleQueryResult createDrizzleQueryResult(ResultSetPacket packet) throws IOException {
        List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();
        for(int i=0;i<packet.getFieldCount();i++) {
            FieldPacket fieldPacket = new FieldPacket(new ReadBuffer(reader));
            fieldPackets.add(fieldPacket);
        }
        ReadBuffer readBuffer = new ReadBuffer(reader);
        if( (readBuffer.getByteAt(0)==(byte)0xfe) && (readBuffer.getLength()<9)) { //check for EOF
        } else {
            throw new IOException("Could not parse result");
        }
        DrizzleQueryResult dqr = new DrizzleQueryResult(fieldPackets);
        while(true) {
            readBuffer = new ReadBuffer(reader);
            if((readBuffer.getByteAt(0)==(byte)0xfe) && (readBuffer.getLength()<9)) { //check for EOF
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
