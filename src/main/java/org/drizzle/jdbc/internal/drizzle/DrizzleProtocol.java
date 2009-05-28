/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle;

import org.drizzle.jdbc.internal.SQLExceptionMapper;
import org.drizzle.jdbc.internal.common.*;
import org.drizzle.jdbc.internal.common.packet.*;
import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.common.packet.commands.ClosePacket;
import org.drizzle.jdbc.internal.common.packet.commands.SelectDBPacket;
import org.drizzle.jdbc.internal.common.packet.commands.StreamedQueryPacket;
import org.drizzle.jdbc.internal.common.query.DrizzleQuery;
import org.drizzle.jdbc.internal.common.query.Query;
import org.drizzle.jdbc.internal.common.queryresults.ColumnInformation;
import org.drizzle.jdbc.internal.common.queryresults.DrizzleQueryResult;
import org.drizzle.jdbc.internal.common.queryresults.DrizzleUpdateResult;
import org.drizzle.jdbc.internal.common.queryresults.QueryResult;
import org.drizzle.jdbc.internal.drizzle.packet.GreetingReadPacket;
import org.drizzle.jdbc.internal.drizzle.packet.commands.ClientAuthPacket;
import org.drizzle.jdbc.internal.drizzle.packet.commands.PingPacket;

import javax.net.SocketFactory;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

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
    private final static Logger log = Logger.getLogger(DrizzleProtocol.class.getName());
    private boolean connected = false;
    private final Socket socket;
    private final BufferedOutputStream writer;
    private final String version;
    private boolean readOnly = false;
    private boolean autoCommit;
    private final String host;
    private final int port;
    private String database;
    private final String username;
    private final String password;
    private final List<Query> batchList;
    private final PacketFetcher packetFetcher;

    /**
     * Get a protocol instance
     *
     * @param host     the host to connect to
     * @param port     the port to connect to
     * @param database the initial database
     * @param username the username
     * @param password the password
     * @throws QueryException if there is a problem reading / sending the packets
     */
    public DrizzleProtocol(String host, int port, String database, String username, String password) throws QueryException {
        this.host = host;
        this.port = port;
        this.database = (database == null ? "" : database);
        this.username = (username == null ? "" : username);
        this.password = (password == null ? "" : password);

        SocketFactory socketFactory = SocketFactory.getDefault();
        try {
            socket = socketFactory.createSocket(host, port);
        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        log.info("Connected to: " + host + ":" + port);
        batchList = new ArrayList<Query>();
        try {
            InputStream reader = socket.getInputStream();
            writer = new BufferedOutputStream(socket.getOutputStream(), 1638);

            GreetingReadPacket greetingPacket = new GreetingReadPacket(reader);
            log.finest("Got greeting packet: " + greetingPacket);
            this.version = greetingPacket.getServerVersion();
            Set<ServerCapabilities> serverCapabilities = greetingPacket.getServerCapabilities();
            serverCapabilities.removeAll(EnumSet.of(ServerCapabilities.SSL, ServerCapabilities.ODBC, ServerCapabilities.NO_SCHEMA));
            serverCapabilities.addAll(EnumSet.of(ServerCapabilities.CONNECT_WITH_DB));
            ClientAuthPacket cap = new ClientAuthPacket(this.username, this.password, this.database, serverCapabilities);
            cap.send(writer);
            log.finest("Sending auth packet");
            packetFetcher = new AsyncPacketFetcher(reader);
            RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacket rp = ResultPacketFactory.createResultPacket(rawPacket);
            if (rp.getResultType() == ResultPacket.ResultType.ERROR) {
                ErrorPacket ep = (ErrorPacket) rp;
                String message = ep.getMessage();
                throw new QueryException("Could not connect: " + message, ep.getErrorNumber(), ep.getSqlState());
            }
            // default when connecting is to have autocommit = 1.
            if (!((OKPacket) rp).getServerStatus().contains(ServerStatus.AUTOCOMMIT))
                setAutoCommit(true);
        } catch (IOException e) {
            close();
            throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        connected = true;
    }

    /**
     * Closes socket and stream readers/writers
     *
     * @throws QueryException if the socket or readers/writes cannot be closed
     */
    public void close() throws QueryException {
        log.info("Closing connection");
        try {
            packetFetcher.close(); // by sending the close packet now, the response will act as a poison pill for the reading thread
            ClosePacket closePacket = new ClosePacket();
            closePacket.send(writer);
            packetFetcher.awaitTermination();
            writer.close();
        } catch (IOException e) {
            throw new QueryException("Could not close socket: " + e.getMessage(), -1, "08000", e);
        } finally {
            try {
                this.connected = false;
                socket.close();
            } catch (IOException e) {
                log.warning("Could not close socket");
            }
        }
    }

    /**
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return !this.connected;
    }


    public void selectDB(String database) throws QueryException {
        log.finest("Selecting db " + database);
        SelectDBPacket packet = new SelectDBPacket(database);
        try {
            packet.send(writer);
            RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacketFactory.createResultPacket(rawPacket);
        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        this.database = database;
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
        log.finest("commiting transaction");
        executeQuery(new DrizzleQuery("COMMIT"));
    }

    public void rollback() throws QueryException {
        log.finest("rolling transaction back");
        executeQuery(new DrizzleQuery("ROLLBACK"));
    }

    public void rollback(String savepoint) throws QueryException {
        log.finest("rolling back to savepoint: " + savepoint);
        executeQuery(new DrizzleQuery("ROLLBACK TO SAVEPOINT " + savepoint));
    }

    public void setSavepoint(String savepoint) throws QueryException {
        log.finest("setting a savepoint named " + savepoint);
        executeQuery(new DrizzleQuery("SAVEPOINT " + savepoint));
    }

    public void releaseSavepoint(String savepoint) throws QueryException {
        log.finest("releasing savepoint named " + savepoint);
        executeQuery(new DrizzleQuery("RELEASE SAVEPOINT " + savepoint));
    }

    public void setAutoCommit(boolean autoCommit) throws QueryException {
        this.autoCommit = autoCommit;
        executeQuery(new DrizzleQuery("SET autocommit=" + (autoCommit ? "1" : "0")));
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
            log.finest("Sent ping packet");
            RawPacket rawPacket = packetFetcher.getRawPacket();
            return ResultPacketFactory.createResultPacket(rawPacket).getResultType() == ResultPacket.ResultType.OK;
        } catch (IOException e) {
            throw new QueryException("Could not ping: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    public QueryResult executeQuery(Query dQuery) throws QueryException {
        log.finest("Executing streamed query: " + dQuery);
        StreamedQueryPacket packet = new StreamedQueryPacket(dQuery);
        int i = 0;
        try {
            packet.send(writer);
        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        RawPacket rawPacket = null;
        try {
            rawPacket = packetFetcher.getRawPacket();
        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rawPacket);

        switch (resultPacket.getResultType()) {
            case ERROR:
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ErrorPacket ep = (ErrorPacket) resultPacket;
                try {
                    dQuery.writeTo(baos);
                    log.warning("Could not execute query " + baos.toString() + ": " + ep.getMessage());
                    throw new QueryException("Could not execute query: " + ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
                } catch (IOException e) {
                    throw new QueryException("Could not execute query: " + ((ErrorPacket) resultPacket).getMessage());
                }
            case OK:
                OKPacket okpacket = (OKPacket) resultPacket;
                QueryResult updateResult = new DrizzleUpdateResult(okpacket.getAffectedRows(),
                        okpacket.getWarnings(),
                        okpacket.getMessage(),
                        okpacket.getInsertId());
                log.finest("OK, " + okpacket.getAffectedRows());
                return updateResult;
            case RESULTSET:
                log.finest("SELECT executed, fetching result set");
                try {
                    return this.createDrizzleQueryResult((ResultSetPacket) resultPacket);
                } catch (IOException e) {
                    throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            default:
                log.severe("Could not parse result...");
                throw new QueryException("Could not parse result");
        }

    }

    /**
     * create a DrizzleQueryResult - precondition is that a result set packet has been read
     *
     * @param packet the result set packet from the server
     * @return a DrizzleQueryResult
     * @throws IOException when something goes wrong while reading/writing from the server
     */
    private QueryResult createDrizzleQueryResult(ResultSetPacket packet) throws IOException {
        List<ColumnInformation> columnInformation = new ArrayList<ColumnInformation>();
        for (int i = 0; i < packet.getFieldCount(); i++) {
            RawPacket rawPacket = packetFetcher.getRawPacket();
            ColumnInformation columnInfo = FieldPacket.columnInformationFactory(rawPacket);
            columnInformation.add(columnInfo);
        }
        packetFetcher.getRawPacket();
        List<List<ValueObject>> valueObjects = new ArrayList<List<ValueObject>>();

        while (true) {
            RawPacket rawPacket = packetFetcher.getRawPacket();
            if (ReadUtil.eofIsNext(rawPacket)) {
                EOFPacket eofPacket = (EOFPacket) ResultPacketFactory.createResultPacket(rawPacket);
                return new DrizzleQueryResult(columnInformation, valueObjects, eofPacket.getWarningCount());
            }
            RowPacket rowPacket = new RowPacket(rawPacket, columnInformation);
            valueObjects.add(rowPacket.getRow());
        }
    }

    public void addToBatch(Query dQuery) {
        log.fine("Adding query to batch");
        batchList.add(dQuery);
    }

    public List<QueryResult> executeBatch() throws QueryException {
        log.fine("executing batch");
        List<QueryResult> retList = new ArrayList<QueryResult>(batchList.size());
        int i = 0;
        for (Query query : batchList) {
            log.finest("executing batch query");
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