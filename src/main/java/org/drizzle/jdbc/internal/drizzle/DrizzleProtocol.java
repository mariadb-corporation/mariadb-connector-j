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
import org.drizzle.jdbc.internal.common.ColumnInformation;
import org.drizzle.jdbc.internal.common.queryresults.DrizzleQueryResult;
import org.drizzle.jdbc.internal.common.queryresults.DrizzleUpdateResult;
import org.drizzle.jdbc.internal.common.queryresults.QueryResult;
import org.drizzle.jdbc.internal.drizzle.packet.GreetingReadPacket;
import org.drizzle.jdbc.internal.drizzle.packet.FieldPacket;
import org.drizzle.jdbc.internal.drizzle.packet.RowPacket;
import org.drizzle.jdbc.internal.drizzle.packet.commands.ClientAuthPacket;
import org.drizzle.jdbc.internal.drizzle.packet.commands.PingPacket;

import javax.net.SocketFactory;
import java.io.*;
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
public final class DrizzleProtocol implements Protocol {
    /**
     * The logger.
     */
    private final static Logger log = Logger.getLogger(DrizzleProtocol.class.getName());
    /**
     * true if we are connected.
     */
    private boolean connected = false;

    /**
     * the socket to use to communicate.
     */
    private final Socket socket;
    /**
     * write to the server using this.
     */
    private final BufferedOutputStream writer;
    /**
     * version of the server.
     */
    private final String serverVersion;
    /**
     * if the connection is read only.
     * //TODO: not used - need to parse queries etc.
     */
    private boolean readOnly = false;
    /**
     * if we are in autocommit mode.
     */
    private boolean autoCommit;
    /**
     * the host we are connected to.
     */
    private final String host;
    /**
     * the port.
     */
    private final int port;
    /**
     * current database.
     */
    private String database;
    /**
     * the username we authenticated as.
     */
    private final String username;
    /**
     * the password used to connect.
     */
    private final String password;
    /**
     * the list of batch queries.
     */
    private final List<Query> batchList;
    /**
     * the packet fetcher. Fetches packets and gives them
     * back to the protocol class.
     */
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
        if (database == null) {
            this.database = "";
        } else {
            this.database = database;
        }
        if (username == null) {
            this.username = "";
        } else {
            this.username = username;
        }
        if (password == null) {
            this.password = "";
        } else {
            this.password = password;
        }

        SocketFactory socketFactory = SocketFactory.getDefault();
        try {
            socket = socketFactory.createSocket(host, port);
        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        log.info("Connected to: " + host + ":" + port);
        batchList = new ArrayList<Query>();
        try {
            BufferedInputStream reader = new BufferedInputStream(socket.getInputStream(), 65536);
            writer = new BufferedOutputStream(socket.getOutputStream(), 65536);
            GreetingReadPacket greetingPacket = new GreetingReadPacket(reader);
            log.finest("Got greeting packet: " + greetingPacket);

            this.serverVersion = greetingPacket.getServerVersion();

            Set<ServerCapabilities> serverCapabilities =
                    EnumSet.of(ServerCapabilities.LONG_PASSWORD,
                               ServerCapabilities.CONNECT_WITH_DB,
                               ServerCapabilities.IGNORE_SPACE,
                               ServerCapabilities.CLIENT_PROTOCOL_41,
                               ServerCapabilities.SECURE_CONNECTION);

            ClientAuthPacket cap = new ClientAuthPacket(this.username,
                                                        this.password,
                                                        this.database,
                                                        serverCapabilities);
            cap.send(writer);

            log.finest("Sending auth packet");
            packetFetcher = new AsyncPacketFetcher(reader);
            packetFetcher.start();
            RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacket rp = ResultPacketFactory.createResultPacket(rawPacket);

            if (rp.getResultType() == ResultPacket.ResultType.ERROR) {
                ErrorPacket ep = (ErrorPacket) rp;
                String message = ep.getMessage();
                throw new QueryException("Could not connect: "
                        + message, ep.getErrorNumber(), ep.getSqlState());
            }
            // default when connecting is to have autocommit = 1.
            if (!((OKPacket) rp).getServerStatus().contains(ServerStatus.AUTOCOMMIT)) {
                setAutoCommit(true);
            }
        } catch (IOException e) {
            close();
            throw new QueryException("Could not connect: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        connected = true;
    }

    /**
     * Closes socket and stream readers/writers.
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
     * has this connection been closed.
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return !this.connected;
    }

    /**
     * selects the database.
     * @param database the database
     * @throws QueryException if we could not select the db.
     */
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

    /**
     * returns the server version.
     * @return the server version.
     */
    public String getServerVersion() {
        return serverVersion;
    }

    /**
     * sets whether this connection is read only.
     * //TODO: use!
     * @param readOnly if the connection should be read only
     */
    public void setReadonly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * returns true if this connection is read only.
     * @return true if the connection is RO.
     */
    public boolean getReadonly() {
        return readOnly;
    }

    /**
     * commits the current transaction.
     * @throws QueryException true if it was not possible to commit the transaction.
     */
    public void commit() throws QueryException {
        log.finest("commiting transaction");
        executeQuery(new DrizzleQuery("COMMIT"));
    }

    /**
     * rolls back the current transaction.
     * @throws QueryException if there is a problem rolling back.
     */
    public void rollback() throws QueryException {
        log.finest("rolling transaction back");
        executeQuery(new DrizzleQuery("ROLLBACK"));
    }

    /**
     * rolls back a transaction to the given savepoint.
     * @param savepoint the save point to roll back to
     * @throws QueryException if there is a problem rolling back.
     */
    public void rollback(String savepoint) throws QueryException {
        log.finest("rolling back to savepoint: " + savepoint);
        executeQuery(new DrizzleQuery("ROLLBACK TO SAVEPOINT " + savepoint));
    }

    /**
     * sets a savepoint with the given name.
     * @param savepoint the save point name
     * @throws QueryException if there is a problem setting the save point.
     */
    public void setSavepoint(String savepoint) throws QueryException {
        log.finest("setting a savepoint named " + savepoint);
        executeQuery(new DrizzleQuery("SAVEPOINT " + savepoint));
    }

    /**
     * releases a save point.
     * @param savepoint the name of the savepoint to release
     * @throws QueryException if there is a problem releasing the savepoint.
     */
    public void releaseSavepoint(String savepoint) throws QueryException {
        log.finest("releasing savepoint named " + savepoint);
        executeQuery(new DrizzleQuery("RELEASE SAVEPOINT " + savepoint));
    }

    /**
     * sets whether statements should be autocommitted.
     * @param autoCommit true if they should be autocommitted
     * @throws QueryException if there is a problem talking to drizzled
     */
    public void setAutoCommit(boolean autoCommit) throws QueryException {
        this.autoCommit = autoCommit;
        String boolStr = "0";
        if(autoCommit) {
            boolStr = "1";
        }
        executeQuery(new DrizzleQuery("SET autocommit=" + boolStr));
    }

    /**
     * if this connection is in autoCommit mode.
     * @return true if we are autoCommiting statements.
     */
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * returns the host we are connected to.
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * returns the current port.
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * returns the current database.
     * @return the database.
     */
    public String getDatabase() {
        return database;
    }

    /**
     * returns the username of the connection.
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * returns the password.
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * pings the server.
     * @return true if it can ping, false otherwise.
     * @throws QueryException there was a problem pinging the server.
     */
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

    /**
     * executes a query.
     * @param dQuery the query to execute
     * @return the result of the query.
     * @throws QueryException if anything goes wrong when sending
     * the query or reading back the results
     */
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
     * create a DrizzleQueryResult - precondition is that a result set packet has been read.
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

    /**
     * adds a query to the batch.
     * //TODO: this needs optimization
     * @param dQuery the query to add
     */
    public void addToBatch(Query dQuery) {
        log.fine("Adding query to batch");
        batchList.add(dQuery);
    }

    /**
     * sends the batch to the server.
     * @return a list of query results.
     * @throws QueryException if something goes wrong with the queries.
     */
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

    /**
     * removes all queries in the batch.
     */
    public void clearBatch() {
        batchList.clear();
    }

    /**
     * not yet supported in drizzle.
     * @param startPos starting position in binlog
     * @param filename binlog file name
     * @return a list of raw binlog entries
     */
    public List<RawPacket> startBinlogDump(int startPos, String filename) {
        return null;
    }

    public SupportedDatabases getDatabaseType() {
        return SupportedDatabases.DRIZZLE;
    }

}