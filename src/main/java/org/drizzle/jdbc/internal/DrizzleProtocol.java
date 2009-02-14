package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.packet.*;
import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.net.Socket;
import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

/**
 * TODO: refactor, clean up
 * TODO: when should i read up the resultset?
 * TODO: thread safety?
 * TODO: exception handling
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 4:06:26 PM
 */
public class DrizzleProtocol implements Protocol {
    private final static Logger log = LoggerFactory.getLogger(DrizzleProtocol.class);
    private boolean connected=false;
    private Socket socket;
    private BufferedInputStream reader;
    private BufferedOutputStream writer;
    private String version;
    private boolean readOnly=false;
    private boolean autoCommit;
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;

    //private ProtocolState protocolState = ProtocolState.NO_TRANSACTION;
    /**
     * Get a protocol instance
     *
     * @param host the host to connect to
     * @param port the port to connect to
     * @param database the initial database
     * @param username the username
     * @param password the password
     * @throws QueryException if there is a problem reading / sending the packets
     */
    public DrizzleProtocol(String host, int port, String database, String username, String password) throws QueryException {
        this.host=host;
        this.port=port;
        this.database=(database==null?"":database);
        this.username=(username==null?"":username);
        this.password=(password==null?"":password);
        SocketFactory socketFactory = SocketFactory.getDefault();
        try {
            socket = socketFactory.createSocket(host,port);
        } catch (IOException e) {
            throw new QueryException("Could not connect socket",e);
        }
        log.info("Connected to: {}:{}",host,port);
        try {
            reader = new BufferedInputStream(socket.getInputStream(),16384);
            writer = new BufferedOutputStream(socket.getOutputStream(),16384);
            this.connect(this.username,this.password,this.database);
        } catch (IOException e) {
            throw new QueryException("Could not connect",e);
        }
    }

    /**
     * Connect to database
     * @param username the username to use
     * @param password the password for the user
     * @param database initial database
     * @throws QueryException ifsomething is wrong while reading / writing streams
     */
    private void connect(String username, String password, String database) throws QueryException {
        this.connected=true;
        byte packetSeqNum = 1;
        GreetingReadPacket greetingPacket = null;
        try {
            greetingPacket = new GreetingReadPacket(reader);
        } catch (IOException e) {
            throw new QueryException("Could not read greeting from server",e);
        }
        this.version=greetingPacket.getServerVersion();
        log.debug("Got greeting packet: {}",greetingPacket);
        ClientAuthPacket cap = new ClientAuthPacket(username,password,database);
        cap.setServerCapabilities(greetingPacket.getServerCapabilities());
        cap.setServerLanguage(greetingPacket.getServerLanguage());
        byte [] a = cap.toBytes(packetSeqNum);
        try {
            writer.write(a);
            writer.flush();
        } catch (IOException e) {
            throw new QueryException("Could not write to server",e);
        }
        log.debug("Sending auth packet: {}",cap);
        ResultPacket resultPacket = null;
        try {
            resultPacket = ResultPacketFactory.createResultPacket(reader);
        } catch (IOException e) {
            throw new QueryException("Could not get result from server",e);
        }
        log.debug("Got result: {}",resultPacket);
         selectDB(database);
         setAutoCommit(true);
    }

    /**
     * Closes socket and stream readers/writers
     * @throws QueryException if the socket or readers/writes cannot be closed
     */
    public void close() throws QueryException {
        log.debug("Closing...");
        try {
            ClosePacket closePacket = new ClosePacket();
            writer.write(closePacket.toBytes((byte)0));
            writer.close();
            reader.close();
            socket.close();
        } catch(IOException e){
            throw new QueryException("Could not close connection",e);
        }
        this.connected=false;
    }

    /**
     *
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return !this.connected;
    }

    /**
     * executes a query, eagerly fetches the results
     * @param query the query to execute
     * @return the query result
     * @throws IOException 
     * @throws SQLException
     */
    public DrizzleQueryResult executeQuery(String query) throws QueryException {

        log.debug("Executing query: {}",query);
        QueryPacket packet = new QueryPacket(query);
        byte packetSeqNum=0;
        byte [] toWrite = packet.toBytes(packetSeqNum);
        ResultPacket resultPacket = null;
        try {
            writer.write(toWrite);
            writer.flush();            
            resultPacket = ResultPacketFactory.createResultPacket(reader);
        } catch (IOException e) {
            throw new QueryException("Could not send query",e);
        }
        switch(resultPacket.getResultType()) {
            case ERROR:
                log.warn("Could not execute query {}: {}",query, ((ErrorPacket)resultPacket).getMessage());
                throw new QueryException("Could not execute query: "+((ErrorPacket)resultPacket).getMessage());
            case OK:
                DrizzleQueryResult dqr = new DrizzleQueryResult();
                OKPacket okpacket = (OKPacket)resultPacket;
                dqr.setUpdateCount((int)okpacket.getAffectedRows());
                dqr.setWarnings(okpacket.getWarnings());
                dqr.setMessage(okpacket.getMessage());
                dqr.setInsertId(okpacket.getInsertId());
                log.debug("OK, {}", okpacket.getAffectedRows());
                return dqr;
            case RESULTSET:
                log.debug("SELECT executed, fetching result set");
                try {
                    return this.createDrizzleQueryResult((ResultSetPacket)resultPacket);
                } catch (IOException e) {
                    throw new QueryException("Could not get query result",e);
                }
            default:
                log.error("Could not parse result...");
                throw new QueryException("Could not parse result");
        }
    }

    /**
     * create a DrizzleQueryResult - precondition is that a result set packet has been read
     * @param packet the result set packet from the server
     * @return a DrizzleQueryResult
     * @throws IOException when something goes wrong while reading/writing from the server
     */
    private DrizzleQueryResult createDrizzleQueryResult(ResultSetPacket packet) throws IOException {
        List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();
        for(int i=0;i<packet.getFieldCount();i++) {
            FieldPacket fieldPacket = new FieldPacket(reader);
            fieldPackets.add(fieldPacket);
        }
        EOFPacket eof = new EOFPacket(reader);
        DrizzleQueryResult dqr = new DrizzleQueryResult(fieldPackets);
        while(true) {
            if(ReadUtil.eofIsNext(reader)) {
                new EOFPacket(reader);
                return dqr;
            }
            RowPacket rowPacket = new RowPacket(reader,packet.getFieldCount());
            dqr.addRow(rowPacket.getRow());
        }
    }
    
    public void selectDB(String database) throws QueryException {
        log.debug("Selecting db {}",database);
        SelectDBPacket packet = new SelectDBPacket(database);
        byte packetSeqNum=0;
        byte [] b = packet.toBytes(packetSeqNum);
        try {
            writer.write(b);
            writer.flush();
            ResultPacketFactory.createResultPacket(reader);
        } catch (IOException e) {
            throw new QueryException("Could not select database",e);
        }
    }

    public String getVersion() {
        return version;
    }

    public void setReadonly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean getReadonly() {
        return readOnly;
    }

    public void commit() throws QueryException {
        log.debug("commiting transaction");
        executeQuery("COMMIT");
    }

    public void rollback() throws QueryException {
        log.debug("rolling transaction back");
        executeQuery("ROLLBACK");
    }

    public void rollback(String savepoint) throws QueryException {
        log.debug("rolling back to savepoint {}",savepoint);
        executeQuery("ROLLBACK TO SAVEPOINT "+savepoint);
    }

    public void setSavepoint(String savepoint) throws QueryException {
        log.debug("setting a savepoint named {}",savepoint);
        executeQuery("SAVEPOINT "+savepoint);
    }
    public void releaseSavepoint(String savepoint) throws QueryException {
        log.debug("releasing savepoint named {}",savepoint);
        executeQuery("RELEASE SAVEPOINT "+savepoint);
    }

    public void setAutoCommit(boolean autoCommit) throws QueryException {
        this.autoCommit = autoCommit;
        executeQuery("SET autocommit="+(autoCommit?"1":"0"));
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean ping() throws QueryException {
        PingPacket pingPacket = new PingPacket();
        try {
            writer.write(pingPacket.toBytes((byte)0));
            writer.flush();
            log.debug("Sent ping packet");
            return ResultPacketFactory.createResultPacket(reader).getResultType()==ResultPacket.ResultType.OK;
        } catch (IOException e) {
            throw new QueryException("Could not ping",e);
        }
    }
}