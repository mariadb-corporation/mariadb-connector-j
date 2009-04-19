package org.drizzle.jdbc.internal.drizzle;

import org.drizzle.jdbc.internal.drizzle.packet.*;
import org.drizzle.jdbc.internal.drizzle.packet.commands.*;
import org.drizzle.jdbc.internal.drizzle.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.common.query.Query;
import org.drizzle.jdbc.internal.common.query.drizzle.DrizzleQuery;
import org.drizzle.jdbc.internal.common.queryresults.*;
import org.drizzle.jdbc.internal.common.PacketFetcher;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.ValueObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.net.Socket;
import java.io.*;
import java.util.*;

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
    private final Socket socket;
    private final BufferedOutputStream writer;
    private final String version;
    private boolean readOnly=false;
    private boolean autoCommit;
    private final String host;
    private final int port;
    private String database;
    private final String username;
    private final String password;
    private final List<Query> batchList;
    private long totalTime=0;
    private int queryCount;
    private PacketFetcher packetFetcher;

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
        batchList=new ArrayList<Query>();
        try {
            InputStream reader = new BufferedInputStream(socket.getInputStream(),1638);
            writer = new BufferedOutputStream(socket.getOutputStream(),1638);

            GreetingReadPacket greetingPacket = new GreetingReadPacket(reader);
            log.debug("Got greeting packet: {}",greetingPacket);
            this.version=greetingPacket.getServerVersion();
            Set<ServerCapabilities> serverCapabilities = greetingPacket.getServerCapabilities();
            serverCapabilities.removeAll(EnumSet.of(ServerCapabilities.SSL, ServerCapabilities.ODBC, ServerCapabilities.NO_SCHEMA));
            serverCapabilities.addAll(EnumSet.of(ServerCapabilities.CONNECT_WITH_DB));
            ClientAuthPacket cap = new ClientAuthPacket(this.username,this.password,this.database,serverCapabilities);
            cap.send(writer);
            log.debug("Sending auth packet: {}",cap);
            packetFetcher = new AsyncPacketFetcher(reader);
            packetFetcher.getRawPacket();
            selectDB(this.database);
            setAutoCommit(true);
        } catch (IOException e) {
            throw new QueryException("Could not connect",e);
        }
    }

    /**
     * Closes socket and stream readers/writers
     * @throws QueryException if the socket or readers/writes cannot be closed
     */
    public void close() throws QueryException {
        log.debug("Closing...");
        try {
            ClosePacket closePacket = new ClosePacket();
            closePacket.send(writer);
            packetFetcher.close();

            writer.close();
            //reader.close();
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
     * create a DrizzleQueryResult - precondition is that a result set packet has been read
     * @param packet the result set packet from the server
     * @return a DrizzleQueryResult
     * @throws IOException when something goes wrong while reading/writing from the server
     */
    private QueryResult createDrizzleQueryResult(ResultSetPacket packet) throws IOException {
        List<ColumnInformation> columnInformation = new ArrayList<ColumnInformation>();
        for(int i=0;i<packet.getFieldCount();i++) {
            RawPacket rawPacket = packetFetcher.getRawPacket();
            ColumnInformation columnInfo = FieldPacket.columnInformationFactory(rawPacket);
            columnInformation.add(columnInfo);
        }
        packetFetcher.getRawPacket();
        List<List<ValueObject>> valueObjects = new ArrayList<List<ValueObject>>();
  
        while(true) {
            RawPacket rawPacket = packetFetcher.getRawPacket();
            if(ReadUtil.eofIsNext(rawPacket)) {
                return new DrizzleQueryResult(columnInformation,valueObjects);
            }
            RowPacket rowPacket = new RowPacket(rawPacket,columnInformation);
            valueObjects.add(rowPacket.getRow());
        }
    }
    
    public void selectDB(String database) throws QueryException {
        log.debug("Selecting db {}",database);
        SelectDBPacket packet = new SelectDBPacket(database);
        try {
            packet.send(writer);
            RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacketFactory.createResultPacket(rawPacket);
        } catch (IOException e) {
            throw new QueryException("Could not select database ",e);
        }
        this.database=database;
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
        executeQuery(new DrizzleQuery("COMMIT"));
    }

    public void rollback() throws QueryException {
        log.debug("rolling transaction back");
        executeQuery(new DrizzleQuery("ROLLBACK"));
    }

    public void rollback(String savepoint) throws QueryException {
        log.debug("rolling back to savepoint {}",savepoint);
        executeQuery(new DrizzleQuery("ROLLBACK TO SAVEPOINT "+savepoint));
    }

    public void setSavepoint(String savepoint) throws QueryException {
        log.debug("setting a savepoint named {}",savepoint);
        executeQuery(new DrizzleQuery("SAVEPOINT "+savepoint));
    }
    public void releaseSavepoint(String savepoint) throws QueryException {
        log.debug("releasing savepoint named {}",savepoint);
        executeQuery(new DrizzleQuery("RELEASE SAVEPOINT "+savepoint));
    }

    public void setAutoCommit(boolean autoCommit) throws QueryException {
        this.autoCommit = autoCommit;
        executeQuery(new DrizzleQuery("SET autocommit="+(autoCommit?"1":"0")));
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
            pingPacket.send(writer);
            log.debug("Sent ping packet");
            RawPacket rawPacket = packetFetcher.getRawPacket();
            return ResultPacketFactory.createResultPacket(rawPacket).getResultType()==ResultPacket.ResultType.OK;
        } catch (IOException e) {
            throw new QueryException("Could not ping",e);
        }
    }

    public QueryResult executeQuery(Query dQuery) throws QueryException {
        log.debug("Executing streamed query: {}",dQuery);
        StreamedQueryPacket packet = new StreamedQueryPacket(dQuery);
        int i=0;
        try {
            packet.send(writer);
        } catch (IOException e) {
            throw new QueryException("Could not send query",e);
        }

        RawPacket rawPacket = packetFetcher.getRawPacket();
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rawPacket);

        switch(resultPacket.getResultType()) {
            case ERROR:
                log.warn("Could not execute query {}: {}",dQuery, ((ErrorPacket)resultPacket).getMessage());
                throw new QueryException("Could not execute query: "+((ErrorPacket)resultPacket).getMessage());
            case OK:
                OKPacket okpacket = (OKPacket)resultPacket;
                QueryResult updateResult = new DrizzleUpdateResult(okpacket.getAffectedRows(),
                                                                            okpacket.getWarnings(),
                                                                            okpacket.getMessage(),
                                                                            okpacket.getInsertId());
                log.debug("OK, {}", okpacket.getAffectedRows());
                return updateResult;
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

    public void addToBatch(Query dQuery) {
        log.info("Adding query to batch");
        batchList.add(dQuery);
    }
    public List<QueryResult> executeBatch() throws QueryException {
        log.info("executing batch");
        List<QueryResult> retList = new ArrayList<QueryResult>(batchList.size());
        int i=0;
        for(Query query : batchList) {
            log.info("executing batch query");
            retList.add(executeQuery(query));
        }
        clearBatch();
        return retList;

    }

    public void clearBatch() {
        batchList.clear();
    }

    public List<RawPacket> startBinlogDump(int startPos, String filename) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}