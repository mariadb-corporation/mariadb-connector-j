/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Stephane Giron, Marc Isambart, Trond Norbye
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc.internal.mysql;

import org.skysql.jdbc.internal.SQLExceptionMapper;
import org.skysql.jdbc.internal.common.*;
import org.skysql.jdbc.internal.common.packet.*;
import org.skysql.jdbc.internal.common.packet.buffer.ReadUtil;
import org.skysql.jdbc.internal.common.packet.commands.ClosePacket;
import org.skysql.jdbc.internal.common.packet.commands.SelectDBPacket;
import org.skysql.jdbc.internal.common.packet.commands.StreamedQueryPacket;
import org.skysql.jdbc.internal.common.query.MySQLQuery;
import org.skysql.jdbc.internal.common.query.Query;
import org.skysql.jdbc.internal.common.queryresults.*;
import org.skysql.jdbc.internal.mysql.packet.MySQLGreetingReadPacket;
import org.skysql.jdbc.internal.mysql.packet.commands.*;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * TODO: refactor, clean up TODO: when should i read up the resultset? TODO: thread safety? TODO: exception handling
 * User: marcuse Date: Jan 14, 2009 Time: 4:06:26 PM
 */
public class MySQLProtocol implements Protocol {
    private final static Logger log = Logger.getLogger(MySQLProtocol.class.getName());
    private boolean connected = false;
    private Socket socket;
    private PacketOutputStream writer;
    private final String version;
    private boolean readOnly = false;
    private final String host;
    private final int port;
    private String database;
    private final String username;
    private final String password;
    private int maxRows;  /* max rows returned by a statement */
    private final List<Query> batchList;
    private SyncPacketFetcher packetFetcher;
    private final Properties info;
    private final long serverThreadId;
    private volatile boolean queryWasCancelled = false;
    private volatile boolean queryTimedOut = false;
    public boolean moreResults = false;
    public StreamingSelectResult activeResult= null;
    /**
     * Get a protocol instance
     *
     * @param host     the host to connect to
     * @param port     the port to connect to
     * @param database the initial database
     * @param username the username
     * @param password the password
     * @param info
     * @throws org.skysql.jdbc.internal.common.QueryException
     *          if there is a problem reading / sending the packets
     */
    public MySQLProtocol(final String host,
                         final int port,
                         final String database,
                         final String username,
                         final String password,
                         Properties info)
            throws QueryException {
        this.info = info;
        this.host = host;
        this.port = port;
        this.database = (database == null ? "" : database);
        this.username = (username == null ? "" : username);
        this.password = (password == null ? "" : password);

	String logLevel = info.getProperty("MySQLProtocolLogLevel");
	if (logLevel != null)
		log.setLevel(Level.parse(logLevel));
	else
		log.setLevel(Level.OFF);

        final SocketFactory socketFactory = SocketFactory.getDefault();
        try {
            // Extract connectTimeout URL parameter
            String connectTimeoutString = info.getProperty("connectTimeout");
            Integer connectTimeout = null;
            if (connectTimeoutString != null) {
                try {
                    connectTimeout = Integer.valueOf(connectTimeoutString);
                } catch (Exception e) {
                    connectTimeout = null;
                }
            }

            // Create socket with timeout if required
            InetSocketAddress sockAddr = new InetSocketAddress(host, port);
            socket = socketFactory.createSocket();
            if (connectTimeout != null) {
                socket.connect(sockAddr, connectTimeout * 1000);
            } else {
                socket.connect(sockAddr);
            }
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + this.host + ":" +
				this.port + ": " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }
        batchList = new ArrayList<Query>();
        try {
            BufferedInputStream reader = new BufferedInputStream(socket.getInputStream(), 32768);
            packetFetcher = new SyncPacketFetcher(reader);
            writer = new PacketOutputStream(socket.getOutputStream());
            RawPacket packet =  packetFetcher.getRawPacket();
            if (ReadUtil.isErrorPacket(packet)) {
                reader.close();
                ErrorPacket errorPacket = (ErrorPacket)ResultPacketFactory.createResultPacket(packet);
                throw new QueryException(errorPacket.getMessage());
            }
            final MySQLGreetingReadPacket greetingPacket = new MySQLGreetingReadPacket(packet);
            this.serverThreadId = greetingPacket.getServerThreadID();
            boolean useCompression = false;

            log.finest("Got greeting packet");
            this.version = greetingPacket.getServerVersion();
            byte packetSeq = 1;
            final Set<MySQLServerCapabilities> capabilities = EnumSet.of(MySQLServerCapabilities.LONG_PASSWORD,
                    MySQLServerCapabilities.IGNORE_SPACE,
                    MySQLServerCapabilities.CLIENT_PROTOCOL_41,
                    MySQLServerCapabilities.TRANSACTIONS,
                    MySQLServerCapabilities.SECURE_CONNECTION,
                    MySQLServerCapabilities.LOCAL_FILES,
                    MySQLServerCapabilities.MULTI_RESULTS,
                    MySQLServerCapabilities.FOUND_ROWS);
            if(info.getProperty("allowMultiQueries") != null) {
                capabilities.add(MySQLServerCapabilities.MULTI_STATEMENTS);
            }
            if(info.getProperty("useCompression") != null) {
                capabilities.add(MySQLServerCapabilities.COMPRESS);
                useCompression = true;
            }
            if(info.getProperty("useSSL") != null && greetingPacket.getServerCapabilities().contains(MySQLServerCapabilities.SSL)) {
                capabilities.add(MySQLServerCapabilities.SSL);
                AbbreviatedMySQLClientAuthPacket amcap = new AbbreviatedMySQLClientAuthPacket(capabilities);
                amcap.send(writer);

                SSLSocketFactory sslSocketFactory = (SSLSocketFactory)SSLSocketFactory.getDefault();
                SSLSocket sslSocket = (SSLSocket)sslSocketFactory.createSocket(socket,
                        socket.getInetAddress().getHostAddress(),
                        socket.getPort(),
                        false);
                sslSocket.setEnabledProtocols(new String [] {"TLSv1"});
                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();
                socket = sslSocket;
                writer = new PacketOutputStream(socket.getOutputStream());
                writer.flush();
                reader = new BufferedInputStream(socket.getInputStream(), 32768);
                packetFetcher = new SyncPacketFetcher(reader);

                packetSeq++;
            } else if(info.getProperty("useSSL") != null){
                throw new QueryException("Trying to connect with ssl, but ssl not enabled in the server");
            }

            // If a database is given, but createDB is not defined or is false,
            // then just try to connect to the given database
            if (this.database != null && !createDB())
                capabilities.add(MySQLServerCapabilities.CONNECT_WITH_DB);

            final MySQLClientAuthPacket cap = new MySQLClientAuthPacket(this.username,
                    this.password,
                    this.database,
                    capabilities,
                    greetingPacket.getSeed(),
                    packetSeq);
            cap.send(writer);
            log.finest("Sending auth packet");

            RawPacket rp = packetFetcher.getRawPacket();

            if ((rp.getByteBuffer().get(0) & 0xFF) == 0xFE) {   // Server asking for old format password
                final MySQLClientOldPasswordAuthPacket oldPassPacket = new MySQLClientOldPasswordAuthPacket(
                        this.password, Utils.copyWithLength(greetingPacket.getSeed(),
                        8), rp.getPacketSeq() + 1);
                oldPassPacket.send(writer);

                rp = packetFetcher.getRawPacket();
            }

            if (useCompression) {
                writer = new PacketOutputStream(new CompressOutputStream(socket.getOutputStream()));
                packetFetcher = new SyncPacketFetcher(new DecompressInputStream(socket.getInputStream()));
            }

            final ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rp);
            if (resultPacket.getResultType() == ResultPacket.ResultType.ERROR) {
                final ErrorPacket ep = (ErrorPacket) resultPacket;
                final String message = ep.getMessage();
                throw new QueryException("Could not connect: " + message);
            }

            // At this point, the driver is connected to the database, if createDB is true, 
            // then just try to create the database and to use it
            if (createDB()) {
                // Try to create the database if it does not exist
                executeQuery(new MySQLQuery("CREATE DATABASE IF NOT EXISTS " + this.database));
                // and switch to this database
                executeQuery(new MySQLQuery("USE " + this.database));
            }

            connected = true;
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + this.host + ":" +
				this.port + ": " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }
    }

    public Properties getInfo() {
        return info;
    }
    void skip() throws IOException, QueryException{
         if (activeResult != null) {
             activeResult.close();
         }

         while (moreResults) {
            QueryResult queryResult =  getMoreResults(true);
         }

    }

    public boolean  hasMoreResults() {
        return moreResults;
    }
    /**
     * Closes socket and stream readers/writers
     *
     * @throws org.skysql.jdbc.internal.common.QueryException
     *          if the socket or readers/writes cannot be closed
     */
    public void close() throws QueryException {
        try {
            socket.shutdownInput();
        } catch (IOException ignored) {
        }
        try {
            final ClosePacket closePacket = new ClosePacket();
            closePacket.send(writer);
            socket.shutdownOutput();
            writer.close();
            packetFetcher.close();
        } catch (IOException e) {
            throw new QueryException("Could not close connection: " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            try {
                this.connected = false;
                socket.close();
            } catch (IOException e) {
                log.warning("Could not close socket");
            }
        }
        this.connected = false;
    }

    /**
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return !this.connected;
    }

    /**
     * create a CachedSelectResult - precondition is that a result set packet has been read
     *
     * @param packet the result set packet from the server
     * @return a CachedSelectResult
     * @throws java.io.IOException when something goes wrong while reading/writing from the server
     */
    private SelectQueryResult createQueryResult(final ResultSetPacket packet, boolean streaming) throws IOException, QueryException {

        StreamingSelectResult streamingResult =   StreamingSelectResult.createStreamingSelectResult(packet, packetFetcher, this);
        if (streaming)
            return streamingResult;

        return CachedSelectResult.createCachedSelectResult(streamingResult);
    }

    public void checkIfCancelled() throws QueryException {
        if (queryWasCancelled) {
            queryWasCancelled = false;
            throw new QueryException("Query was cancelled by another thread", (short) -1, "JZ0001");
        }
        if (queryTimedOut) {
            queryTimedOut = false;
            throw new QueryException("Query timed out", (short) -1, "JZ0002");
        }
    }

    public void selectDB(final String database) throws QueryException {
        log.finest("Selecting db " + database);
        final SelectDBPacket packet = new SelectDBPacket(database);
        try {
            packet.send(writer);
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacketFactory.createResultPacket(rawPacket);
        } catch (IOException e) {
            throw new QueryException("Could not select database: " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }
        this.database = database;
    }

    public String getServerVersion() {
        return version;
    }

    public void setReadonly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean getReadonly() {
        return readOnly;
    }

    public void commit() throws QueryException {
        log.finest("commiting transaction");
        executeQuery(new MySQLQuery("COMMIT"), false);
    }

    public void rollback() throws QueryException {
        log.finest("rolling transaction back");
        executeQuery(new MySQLQuery("ROLLBACK"));
    }

    public void rollback(final String savepoint) throws QueryException {
        log.finest("rolling back to savepoint " + savepoint);
        executeQuery(new MySQLQuery("ROLLBACK TO SAVEPOINT " + savepoint));
    }

    public void setSavepoint(final String savepoint) throws QueryException {
        executeQuery(new MySQLQuery("SAVEPOINT " + savepoint));
    }

    public void releaseSavepoint(final String savepoint) throws QueryException {
        executeQuery(new MySQLQuery("RELEASE SAVEPOINT " + savepoint));
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
        final MySQLPingPacket pingPacket = new MySQLPingPacket();
        try {
            pingPacket.send(writer);
            log.finest("Sent ping packet");
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            return ResultPacketFactory.createResultPacket(rawPacket).getResultType() == ResultPacket.ResultType.OK;
        } catch (IOException e) {
            throw new QueryException("Could not ping: " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }
    }

    public QueryResult executeQuery(Query dQuery)  throws QueryException{
       return executeQuery(dQuery, false);
    }


    public QueryResult getResult(Query dQuery, boolean streaming) throws QueryException{
             RawPacket rawPacket;
        ResultPacket resultPacket;
        try {
            rawPacket = packetFetcher.getRawPacket();
            resultPacket = ResultPacketFactory.createResultPacket(rawPacket);

            if (resultPacket.getResultType() == ResultPacket.ResultType.LOCALINFILE) {
                // Server request the local file (LOCAL DATA LOCAL INFILE)
                LocalInfilePacket localInfilePacket= (LocalInfilePacket)resultPacket;
                log.fine("sending local file " + localInfilePacket.getFileName());
                writer.sendFile(new FileInputStream(localInfilePacket.getFileName()),rawPacket.getPacketSeq()+1);
                rawPacket = packetFetcher.getRawPacket();
                resultPacket = ResultPacketFactory.createResultPacket(rawPacket);
            }
        } catch (IOException e) {
            throw new QueryException("Could not read resultset: " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }

        switch (resultPacket.getResultType()) {
            case ERROR:
                this.moreResults = false;
                final ErrorPacket ep = (ErrorPacket) resultPacket;
                checkIfCancelled();
                if (dQuery != null) {
                    log.warning("Could not execute query " + dQuery + ": " + ((ErrorPacket) resultPacket).getMessage());
                } else {
                    log.warning("Got error from server: " + ((ErrorPacket) resultPacket).getMessage());
                }
                throw new QueryException(ep.getMessage(),
                        ep.getErrorNumber(),
                        ep.getSqlState());
            case OK:
                final OKPacket okpacket = (OKPacket) resultPacket;
                this.moreResults = okpacket.getServerStatus().contains(ServerStatus.MORE_RESULTS_EXISTS);
                final QueryResult updateResult = new UpdateResult(okpacket.getAffectedRows(),
                        okpacket.getWarnings(),
                        okpacket.getMessage(),
                        okpacket.getInsertId());
                log.fine("OK, " + okpacket.getAffectedRows());
                return updateResult;
            case RESULTSET:
                log.fine("SELECT executed, fetching result set");
                ResultSetPacket resultSetPacket = (ResultSetPacket)resultPacket;
                try {
                    return this.createQueryResult(resultSetPacket, streaming);
                } catch (IOException e) {
                    throw new QueryException("Could not read result set: " + e.getMessage(),
                            -1,
                            SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                            e);
                }
            default:
                log.severe("Could not parse result..." + resultPacket.getResultType());
                throw new QueryException("Could not parse result", (short) -1, SQLExceptionMapper.SQLStates.INTERRUPTED_EXCEPTION.getSqlState());
        }
    }

    public QueryResult executeQuery(final Query dQuery, boolean streaming) throws QueryException {
        dQuery.validate();
        log.finest("Executing streamed query: " + dQuery);
        this.moreResults = false;
        final StreamedQueryPacket packet = new StreamedQueryPacket(dQuery);

        try {
            packet.send(writer);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }
        return getResult(dQuery, streaming);
    }

    public void addToBatch(final Query dQuery) {
        batchList.add(dQuery);
    }

    public synchronized List<QueryResult> executeBatch() throws QueryException {
        final List<QueryResult> retList = new ArrayList<QueryResult>(batchList.size());

        for (final Query query : batchList) {
            retList.add(executeQuery(query));
        }
        clearBatch();
        return retList;

    }

    public void clearBatch() {
        batchList.clear();
    }

    public List<RawPacket> startBinlogDump(final int startPos, final String filename) throws BinlogDumpException {
        final MySQLBinlogDumpPacket mbdp = new MySQLBinlogDumpPacket(startPos, filename);
        try {
            mbdp.send(writer);
            final List<RawPacket> rpList = new LinkedList<RawPacket>();
            while (true) {
                final RawPacket rp = this.packetFetcher.getRawPacket();
                if (ReadUtil.eofIsNext(rp)) {
                    return rpList;
                }
                rpList.add(rp);
            }
        } catch (IOException e) {
            throw new BinlogDumpException("Could not read binlog", e);
        }
    }

    public SupportedDatabases getDatabaseType() {
        return SupportedDatabases.fromVersionString(version);
    }

    public boolean supportsPBMS() {
        return info != null && info.getProperty("enableBlobStreaming", "").equalsIgnoreCase("true");
    }

    public String getServerVariable(String variable) throws QueryException {
        CachedSelectResult qr = (CachedSelectResult) executeQuery(new MySQLQuery("select @@" + variable));
        try {
            if (!qr.next()) {
                throw new QueryException("Could not get variable: " + variable);
            }
        }
        catch (IOException ioe ){
            throw new QueryException(ioe.getMessage(), 0, "HYOOO", ioe);
        }


        try {
            String value = qr.getValueObject(0).getString();
            return value;
        } catch (NoSuchColumnException e) {
            throw new QueryException("Could not get variable: " + variable);
        }
    }


    /**
     * cancels the current query - clones the current protocol and executes a query using the new connection
     * <p/>
     * thread safe
     *
     * @throws QueryException
     */
    public void cancelCurrentQuery() throws QueryException {
        Protocol copiedProtocol = new MySQLProtocol(host, port, database, username, password, info);
        queryWasCancelled = true;
        copiedProtocol.executeQuery(new MySQLQuery("KILL QUERY " + serverThreadId));
        copiedProtocol.close();
    }

    public void timeOut() throws QueryException {
        Protocol copiedProtocol = new MySQLProtocol(host, port, database, username, password, info);
        queryTimedOut = true;
        copiedProtocol.executeQuery(new MySQLQuery("KILL QUERY " + serverThreadId));
        copiedProtocol.close();

    }

    public boolean createDB() {
        return info != null
                && info.getProperty("createDB", "").equalsIgnoreCase("true");
    }



    public QueryResult getMoreResults(boolean streaming) throws QueryException {
        if(!moreResults)
            return null;
        return getResult(null, streaming);
    }

    public static String hexdump(byte[] buffer, int offset) {
        StringBuffer dump = new StringBuffer();
        if ((buffer.length - offset) > 0) {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < buffer.length; i++) {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    public static String hexdump(ByteBuffer bb, int offset) {
        byte[] b = new byte[bb.remaining()];
        bb.mark();
        bb.get(b);
        bb.reset();
        return hexdump(b, offset);
    }


    public boolean hasUnreadData() {
        return (activeResult != null);
    }

    public void setMaxRows(int max) throws QueryException{
        if (maxRows != max) {
            if (max == 0) {
                executeQuery(new MySQLQuery("set @@SQL_SELECT_LIMIT=DEFAULT"));
            } else {
                executeQuery(new MySQLQuery("set @@SQL_SELECT_LIMIT=" + max));
            }
            maxRows = max;
        }
    }
}
