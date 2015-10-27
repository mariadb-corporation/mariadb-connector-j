/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:


Copyright (c) 2009-2011, Marcus Eriksson, Stephane Giron, Marc Isambart, Trond Norbye

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.ExceptionMapper;
import org.mariadb.jdbc.internal.common.*;
import org.mariadb.jdbc.internal.common.packet.*;
import org.mariadb.jdbc.internal.common.packet.buffer.ReadUtil;
import org.mariadb.jdbc.internal.common.packet.buffer.Reader;
import org.mariadb.jdbc.internal.common.packet.commands.ChangeDbPacket;
import org.mariadb.jdbc.internal.common.packet.commands.ClosePacket;
import org.mariadb.jdbc.internal.common.packet.commands.StreamedQueryPacket;
import org.mariadb.jdbc.internal.common.query.MariaDbQuery;
import org.mariadb.jdbc.internal.common.query.Query;
import org.mariadb.jdbc.internal.common.query.parameters.LongDataParameterHolder;
import org.mariadb.jdbc.internal.common.query.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.common.queryresults.*;
import org.mariadb.jdbc.internal.mysql.listener.Listener;
import org.mariadb.jdbc.internal.mysql.listener.tools.SearchFilter;
import org.mariadb.jdbc.internal.mysql.packet.GreetingReadPacket;
import org.mariadb.jdbc.internal.mysql.packet.commands.*;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;


public class MariaDbProtocol implements Protocol {
    protected final ReentrantLock lock;
    protected final UrlParser urlParser;
    private final String username;
    private final String password;
    public boolean moreResults = false;
    public boolean hasWarnings = false;
    public StreamingSelectResult activeResult = null;
    public int datatypeMappingFlags;
    public short serverStatus;
    protected Socket socket;
    protected PacketOutputStream writer;
    protected boolean readOnly = false;
    protected SyncPacketFetcher packetFetcher;
    protected HostAddress currentHost;
    protected FailoverProxy proxy;
    boolean hostFailed;
    private boolean connected = false;
    private boolean explicitClosed = false;
    private String version;
    private String database;
    private int maxRows;  /* max rows returned by a statement */
    private long serverThreadId;
    private int majorVersion;
    private int minorVersion;
    private int patchVersion;
    private byte serverLanguage;
    private MariaDbCharset charset;
    private int transactionIsolationLevel = 0;
    private PrepareStatementCache prepareStatementCache;
    private Map<String, String> serverData;
    private InputStream localInfileInputStream;
    private Calendar cal;


    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL infos
     * @param lock the lock for thread synchronisation
     */

    public MariaDbProtocol(final UrlParser urlParser, final ReentrantLock lock) {
        this.lock = lock;
        this.urlParser = urlParser;
        this.database = (urlParser.getDatabase() == null ? "" : urlParser.getDatabase());
        this.username = (urlParser.getUsername() == null ? "" : urlParser.getUsername());
        this.password = (urlParser.getPassword() == null ? "" : urlParser.getPassword());
        if (urlParser.getOptions().cachePrepStmts) {
            prepareStatementCache = PrepareStatementCache.newInstance(urlParser.getOptions().prepStmtCacheSize);
        }
        setDatatypeMappingFlags();
    }

    /**
     * Get new instance.
     *
     * @param proxy proxy
     * @param urlParser url connection object
     * @return new instance
     */
    public static MariaDbProtocol getNewProtocol(FailoverProxy proxy, UrlParser urlParser) {
        MariaDbProtocol newProtocol = new MariaDbProtocol(urlParser, proxy.lock);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }

    /**
     * loop until found the failed connection.
     *
     * @param listener current listener
     * @param addresses list of HostAddress to loop
     * @param blacklist current blacklist
     * @param searchFilter search parameter
     * @throws QueryException if not found
     */
    public static void loop(Listener listener, final List<HostAddress> addresses, Map<HostAddress, Long> blacklist, SearchFilter searchFilter)
            throws QueryException {
//        if (log.isDebugEnabled()) {
//            log.debug("searching for master:" + searchFilter.isSearchForMaster() + " replica:" + searchFilter.isSearchForSlave()
// + " addresses:" + addresses);
//        }

        MariaDbProtocol protocol;
        List<HostAddress> loopAddresses = new LinkedList<>(addresses);
        int maxConnectionTry = listener.getRetriesAllDown();
        QueryException lastQueryException = null;
        while (!loopAddresses.isEmpty() || (!searchFilter.isUniqueLoop() && maxConnectionTry > 0)) {
            protocol = getNewProtocol(listener.getProxy(), listener.getUrlParser());

            if (listener.isExplicitClosed()) {
                return;
            }
            maxConnectionTry--;

            try {
                protocol.setHostAddress(loopAddresses.get(0));
                loopAddresses.remove(0);

//                if (log.isDebugEnabled()) log.debug("trying to connect to " + protocol.getHostAddress());
                protocol.connect();
                blacklist.remove(protocol.getHostAddress());
//                if (log.isDebugEnabled()) log.debug("connected to primary " + protocol.getHostAddress());
                listener.foundActiveMaster(protocol);
                return;

            } catch (QueryException e) {
                blacklist.put(protocol.getHostAddress(), System.currentTimeMillis());
//                if (log.isDebugEnabled()) log.debug("Could not connect to " + protocol.getHostAddress() + " searching: " + searchFilter
// + " error: " + e.getMessage());
                lastQueryException = e;
            }

            //loop is set so
            if (loopAddresses.isEmpty() && !searchFilter.isUniqueLoop() && maxConnectionTry > 0) {
                loopAddresses = new LinkedList<>(addresses);
            }
        }
        if (lastQueryException != null) {
            throw new QueryException("No active connection found for master", lastQueryException.getErrorCode(), lastQueryException.getSqlState(),
                    lastQueryException);
        }
        throw new QueryException("No active connection found for master");
    }


    /**
     * Hexdump.
     *
     * @param buffer byte array
     * @param offset offset
     * @return String
     */
    public static String hexdump(byte[] buffer, int offset) {
        StringBuffer dump = new StringBuffer();
        if ((buffer.length - offset) > 0) {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < buffer.length; i++) {
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    /**
     * Hexdump.
     *
     * @param bb bytebuffer
     * @param offset offset
     * @return String
     */
    public static String hexdump(ByteBuffer bb, int offset) {
        byte[] bit = new byte[bb.remaining()];
        bb.mark();
        bb.get(bit);
        bb.reset();
        return hexdump(bit, offset);
    }

    /**
     * Closes socket and stream readers/writers Attempts graceful shutdown.
     */
    @Override
    public void close() {
        if (lock != null) {
            lock.lock();
        }
        try {
            /* If a streaming result set is open, close it.*/
            skip();
        } catch (Exception e) {
            /* eat exception */
        }
        try {
//            if (log.isTraceEnabled()) log.trace("Closing connection  " + currentHost);
            if (urlParser.getOptions().cachePrepStmts) {
                prepareStatementCache.clear();
            }
            close(packetFetcher, writer, socket);
        } catch (Exception e) {
            // socket is closed, so it is ok to ignore exception
//            log.debug("got exception " + e + " while closing connection");
        } finally {
            this.connected = false;

            if (lock != null) {
                lock.unlock();
            }
        }
    }

    protected static void close(PacketFetcher fetcher, PacketOutputStream packetOutputStream, Socket socket) throws QueryException {
        ClosePacket closePacket = new ClosePacket();
        try {
            try {
                closePacket.send(packetOutputStream);
                socket.shutdownOutput();
                socket.setSoTimeout(3);
                InputStream is = socket.getInputStream();
                while (is.read() != -1) {
                }
            } catch (Throwable t) {
            }
            packetOutputStream.close();
            fetcher.close();
        } catch (IOException e) {
            throw new QueryException("Could not close connection: " + e.getMessage(),
                    -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
//                log.warn("Could not close socket");
            }
        }
    }

    private SSLSocketFactory getSslSocketFactory() throws QueryException {
        if (!urlParser.getOptions().trustServerCertificate
                && urlParser.getOptions().serverSslCert == null) {
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            X509TrustManager[] trustManager = {new MyX509TrustManager(urlParser.getOptions())};
            sslContext.init(null, trustManager, null);
            return sslContext.getSocketFactory();
        } catch (Exception e) {
            throw new QueryException(e.getMessage(), 0, "HY000", e);
        }

    }

    /**
     * Connect to currentHost.
     *
     * @throws QueryException exception
     */
    public void connect() throws QueryException {
        if (!isClosed()) {
            close();
        }
        try {
            connect(currentHost.host, currentHost.port);
            return;
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + currentHost + "." + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * Connect the client and perform handshake
     *
     * @throws QueryException : handshake error, e.g wrong user or password
     * @throws IOException : connection error (host/port not available)
     */
    private void connect(String host, int port) throws QueryException, IOException {

        SocketFactory socketFactory;
        String socketFactoryName = urlParser.getOptions().socketFactory;
        if (socketFactoryName != null) {
            try {
                socketFactory = (SocketFactory) (Class.forName(socketFactoryName).newInstance());
            } catch (Exception sfex) {
//                log.debug("Failed to create socket factory " + socketFactoryName);
                socketFactory = SocketFactory.getDefault();
            }
        } else {
            socketFactory = SocketFactory.getDefault();
        }

        // Create socket with timeout if required
        if (urlParser.getOptions().pipe != null) {
            socket = new org.mariadb.jdbc.internal.mysql.NamedPipeSocket(host, urlParser.getOptions().pipe);
        } else if (urlParser.getOptions().localSocket != null) {
            try {
                socket = new org.mariadb.jdbc.internal.mysql.UnixDomainSocket(urlParser.getOptions().localSocket);
            } catch (RuntimeException re) {
                //  could be e.g library loading error
                throw new IOException(re.getMessage(), re.getCause());
            }
        } else if (urlParser.getOptions().sharedMemory != null) {
            try {
                socket = new SharedMemorySocket(urlParser.getOptions().sharedMemory);
            } catch (RuntimeException re) {
                //  could be e.g library loading error
                throw new IOException(re.getMessage(), re.getCause());
            }
        } else {
            socket = socketFactory.createSocket();
        }

        try {
            if (urlParser.getOptions().tcpNoDelay) {
                socket.setTcpNoDelay(urlParser.getOptions().tcpNoDelay);
            } else {
                socket.setTcpNoDelay(true);
            }

            if (urlParser.getOptions().tcpKeepAlive) {
                socket.setKeepAlive(true);
            }
            if (urlParser.getOptions().tcpRcvBuf != null) {
                socket.setReceiveBufferSize(urlParser.getOptions().tcpRcvBuf);
            }
            if (urlParser.getOptions().tcpSndBuf != null) {
                socket.setSendBufferSize(urlParser.getOptions().tcpSndBuf);
            }
            if (urlParser.getOptions().tcpAbortiveClose) {
                socket.setSoLinger(true, 0);
            }
        } catch (Exception e) {
//            if (log.isDebugEnabled())log.debug("Failed to set socket option: " + e.getLocalizedMessage());
        }

        // Bind the socket to a particular interface if the connection property
        // localSocketAddress has been defined.
        if (urlParser.getOptions().localSocketAddress != null) {
            InetSocketAddress localAddress = new InetSocketAddress(urlParser.getOptions().localSocketAddress, 0);
            socket.bind(localAddress);
        }

        if (!socket.isConnected()) {
            InetSocketAddress sockAddr = new InetSocketAddress(host, port);
            if (urlParser.getOptions().connectTimeout != null) {
                socket.connect(sockAddr, urlParser.getOptions().connectTimeout);
            } else {
                socket.connect(sockAddr);
            }
        }

        // Extract socketTimeout URL parameter
        if (urlParser.getOptions().socketTimeout != null) {
            socket.setSoTimeout(urlParser.getOptions().socketTimeout);
        }

        try {
            InputStream reader;
            reader = new BufferedInputStream(socket.getInputStream(), 32768);
            packetFetcher = new SyncPacketFetcher(reader);
            writer = new PacketOutputStream(socket.getOutputStream());
            ByteBuffer byteBuffer = packetFetcher.getReusableBuffer();
            if (ReadUtil.isErrorPacket(byteBuffer)) {
                reader.close();
                ErrorPacket errorPacket = (ErrorPacket) ResultPacketFactory.createResultPacket(byteBuffer);
                throw new QueryException(errorPacket.getMessage());
            }

            final GreetingReadPacket greetingPacket = new GreetingReadPacket(byteBuffer);
            this.serverThreadId = greetingPacket.getServerThreadId();
            this.serverLanguage = greetingPacket.getServerLanguage();
            this.charset = CharsetUtils.getServerCharset(serverLanguage);
            this.version = greetingPacket.getServerVersion();
            parseVersion();
            byte packetSeq = 1;
            int capabilities =
                    MariaDbServerCapabilities.LONG_PASSWORD
                            | MariaDbServerCapabilities.IGNORE_SPACE
                            | MariaDbServerCapabilities.CLIENT_PROTOCOL_41
                            | MariaDbServerCapabilities.TRANSACTIONS
                            | MariaDbServerCapabilities.SECURE_CONNECTION
                            | MariaDbServerCapabilities.LOCAL_FILES
                            | MariaDbServerCapabilities.MULTI_RESULTS
                            | MariaDbServerCapabilities.FOUND_ROWS;


            if (urlParser.getOptions().allowMultiQueries || (urlParser.getOptions().rewriteBatchedStatements)) {
                capabilities |= MariaDbServerCapabilities.MULTI_STATEMENTS;
            }

            if (urlParser.getOptions().useCompression) {
                capabilities |= MariaDbServerCapabilities.COMPRESS;
            }
            if (urlParser.getOptions().interactiveClient) {
                capabilities |= MariaDbServerCapabilities.CLIENT_INTERACTIVE;
            }

            // If a database is given, but createDatabaseIfNotExist is not defined or is false,
            // then just try to connect to the given database
            if (database != null && !urlParser.getOptions().createDatabaseIfNotExist) {
                capabilities |= MariaDbServerCapabilities.CONNECT_WITH_DB;
            }

            if (urlParser.getOptions().useSsl && (greetingPacket.getServerCapabilities() & MariaDbServerCapabilities.SSL) != 0) {
                capabilities |= MariaDbServerCapabilities.SSL;
                AbbreviatedMariaDbClientAuthPacket amcap = new AbbreviatedMariaDbClientAuthPacket(capabilities);
                amcap.send(writer);

                SSLSocketFactory sslSocketFactory = getSslSocketFactory();
                SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(socket,
                        socket.getInetAddress().getHostAddress(), socket.getPort(), true);

                sslSocket.setEnabledProtocols(new String[]{"TLSv1"});
                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();
                socket = sslSocket;
                writer = new PacketOutputStream(socket.getOutputStream());
                reader = new BufferedInputStream(socket.getInputStream(), 32768);
                packetFetcher = new SyncPacketFetcher(reader);

                packetSeq++;
            } else if (urlParser.getOptions().useSsl) {
                throw new QueryException("Trying to connect with ssl, but ssl not enabled in the server");
            }

            final MariaDbClientAuthPacket cap = new MariaDbClientAuthPacket(this.username,
                    this.password,
                    database,
                    capabilities,
                    decideLanguage(),
                    greetingPacket.getSeed(),
                    packetSeq);
            cap.send(writer);

            RawPacket rp = packetFetcher.getRawPacket();

            if ((rp.getByteBuffer().get(0) & 0xFF) == 0xFE) {   // Server asking for old format password
                final MariaDbClientOldPasswordAuthPacket oldPassPacket = new MariaDbClientOldPasswordAuthPacket(
                        this.password, Utils.copyWithLength(greetingPacket.getSeed(),
                        8), rp.getPacketSeq() + 1);
                oldPassPacket.send(writer);

                rp = packetFetcher.getRawPacket();
            }

            checkErrorPacket(rp.getByteBuffer());
            ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rp.getByteBuffer());
            OkPacket ok = (OkPacket) resultPacket;
            serverStatus = ok.getServerStatus();

            if (urlParser.getOptions().useCompression) {
                writer.setUseCompression(true);
                packetFetcher = new SyncPacketFetcher(new DecompressInputStream(socket.getInputStream()));
            }

            // In JDBC, connection must start in autocommit mode.
            if ((serverStatus & ServerStatus.AUTOCOMMIT) == 0) {
                executeQuery(new MariaDbQuery("set autocommit=1"));
            }

            if (urlParser.getOptions().sessionVariables != null) {
                executeQuery(new MariaDbQuery("set session " + urlParser.getOptions().sessionVariables));
            }

            loadServerData();

            writer.setMaxAllowedPacket(Integer.parseInt(serverData.get("max_allowed_packet")));

            // At this point, the driver is connected to the database, if createDB is true,
            // then just try to create the database and to use it
            if (checkIfMaster()) {
                if (urlParser.getOptions().createDatabaseIfNotExist) {
                    // Try to create the database if it does not exist
                    String quotedDb = MariaDbConnection.quoteIdentifier(this.database);
                    executeQuery(new MariaDbQuery("CREATE DATABASE IF NOT EXISTS " + quotedDb));
                    executeQuery(new MariaDbQuery("USE " + quotedDb));
                }
            }

            activeResult = null;
            moreResults = false;
            hasWarnings = false;
            connected = true;
            hostFailed = false;

            loadCalendar();

        } catch (IOException e) {
            throw new QueryException("Could not connect to " + host + ":" + port + ": " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

    }

    private void loadCalendar() throws QueryException {
        String timeZone = null;
        if (getOptions().serverTimezone != null) {
            timeZone = getOptions().serverTimezone;
        }

        if (timeZone == null) {
            timeZone = getServerData("time_zone");
            if (timeZone != null) {
                if ("SYSTEM".equals(timeZone)) {
                    timeZone = getServerData("system_time_zone");
                }
            }
        }
        //handle custom timezone id
        if (timeZone != null) {
            if (timeZone.length() >= 2 && (timeZone.startsWith("+") || timeZone.startsWith("-")) && Character.isDigit(timeZone.charAt(1))) {
                timeZone = "GMT" + timeZone;
            }
        }
        try {
            TimeZone tz = Utils.getTimeZone(timeZone);
            cal = Calendar.getInstance(tz);
        } catch (SQLException e) {
            cal = null;
            if (!getOptions().useLegacyDatetimeCode) {
                if (getOptions().serverTimezone != null) {
                    throw new QueryException("The server time_zone '" + timeZone + "' defined in the 'serverTimezone' parameter cannot be parsed "
                            + "by java TimeZone implementation. See java.util.TimeZone#getAvailableIDs() for available TimeZone, depending on your "
                            + "JRE implementation.", 0, "01S00");
                } else {
                    throw new QueryException("The server time_zone '" + timeZone + "' cannot be parsed. The server time zone must defined in the "
                            + "jdbc url string with the 'serverTimezone' parameter (or server time zone must be defined explicitly).  See "
                            + "java.util.TimeZone#getAvailableIDs() for available TimeZone, depending on your JRE implementation.", 0, "01S00");
                }
            }
        }

    }

    private void loadServerData() throws QueryException, IOException {
        serverData = new TreeMap<>();
        SelectQueryResult qr = null;
        try {
            qr = executeSingleInternalQuery(new MariaDbQuery("SELECT "
                    + "@@max_allowed_packet, "
                    + "@@system_time_zone, "
                    + "@@time_zone"));
            if (qr.next()) {
                serverData.put("max_allowed_packet", qr.getValueObject(0).getString());
                serverData.put("system_time_zone", qr.getValueObject(1).getString());
                serverData.put("time_zone", qr.getValueObject(2).getString());
            }
        } finally {
            if (qr != null) {
                qr.close();
            }
        }
    }

    public String getServerData(String code) {
        return serverData.get(code);
    }

    public boolean checkIfMaster() throws QueryException {
        return isMasterConnection();
    }

    private boolean isServerLanguageUtf8mb4(byte serverLanguage) {
        Byte[] utf8mb4Languages = {
                (byte) 45, (byte) 46, (byte) 224, (byte) 225, (byte) 226, (byte) 227, (byte) 228,
                (byte) 229, (byte) 230, (byte) 231, (byte) 232, (byte) 233, (byte) 234, (byte) 235,
                (byte) 236, (byte) 237, (byte) 238, (byte) 239, (byte) 240, (byte) 241, (byte) 242,
                (byte) 243, (byte) 245
        };
        return Arrays.asList(utf8mb4Languages).contains(serverLanguage);
    }

    private byte decideLanguage() {
        byte result = (isServerLanguageUtf8mb4(this.serverLanguage) ? this.serverLanguage : 33);
        return result;
    }

    void checkErrorPacket(ByteBuffer byteBuffer) throws QueryException {
        if (byteBuffer.get(0) == -1) {
            ErrorPacket ep = new ErrorPacket(byteBuffer);
            String message = ep.getMessage();
            throw new QueryException("Could not connect: " + message, ep.getErrorNumber(), ep.getSqlState());
        }
    }

    void readEofPacket() throws QueryException, IOException {
        ByteBuffer byteBuffer = packetFetcher.getReusableBuffer();
        checkErrorPacket(byteBuffer);
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(byteBuffer);
        if (resultPacket.getResultType() != ResultPacket.ResultType.EOF) {
            throw new QueryException("Unexpected packet type " + resultPacket.getResultType()
                    + "insted of EOF");
        }
        EndOfFilePacket eof = (EndOfFilePacket) resultPacket;
        this.hasWarnings = eof.getWarningCount() > 0;
        this.serverStatus = eof.getStatusFlags();
    }

    @Override
    public PrepareResult prepare(String sql) throws QueryException {
        try {
            if (urlParser.getOptions().cachePrepStmts) {
                if (prepareStatementCache.containsKey(sql)) {
//                    log.debug("using already cached prepared statement");
                    PrepareResult pr = prepareStatementCache.get(sql);
                    pr.addUse();
                    return pr;
                }
            }

//            log.debug("creating new prepared statement");
            SendPrepareStatementPacket sendPrepareStatementPacket = new SendPrepareStatementPacket(sql);
            sendPrepareStatementPacket.send(writer);

            ByteBuffer byteBuffer = packetFetcher.getReusableBuffer();

            if (byteBuffer.get(0) == -1) {
                ErrorPacket ep = new ErrorPacket(byteBuffer);
                String message = ep.getMessage();
                throw new QueryException("Error preparing query: " + message, ep.getErrorNumber(), ep.getSqlState());
            }


            byte bit = byteBuffer.get(0);
            if (bit == 0) {
                /* Prepared Statement OK */
                Reader reader = new Reader(byteBuffer);
                reader.readByte(); /* skip field count */
                final int statementId = reader.readInt();
                final int numColumns = reader.readShort();
                final int numParams = reader.readShort();
                reader.readByte(); // reserved
                this.hasWarnings = reader.readShort() > 0;
                ColumnInformation[] params = new ColumnInformation[numParams];
                if (numParams > 0) {
                    for (int i = 0; i < numParams; i++) {
                        params[i] = new ColumnInformation(packetFetcher.getRawPacket().getByteBuffer());
                    }
                    readEofPacket();
                }
                ColumnInformation[] columns = new ColumnInformation[numColumns];
                if (numColumns > 0) {
                    for (int i = 0; i < numColumns; i++) {
                        columns[i] = new ColumnInformation(packetFetcher.getRawPacket().getByteBuffer());
                    }
                    readEofPacket();
                }
                PrepareResult prepareResult = new PrepareResult(statementId, columns, params);
                if (urlParser.getOptions().cachePrepStmts) {
                    if (sql != null && sql.length() < urlParser.getOptions().prepStmtCacheSqlLimit) {
                        prepareStatementCache.putIfNone(sql, prepareResult);
                    }
                }
//                if (log.isDebugEnabled()) log.debug("prepare statementId : " + prepareResult.statementId);
                return prepareResult;
            } else {
                throw new QueryException("Unexpected packet returned by server, first byte " + bit);
            }
        } catch (IOException e) {
            throw new QueryException(e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }
    }

    @Override
    public void closePreparedStatement(int statementId) throws QueryException {
        lock.lock();
        try {
            writer.startPacket(0);
            writer.write(0x19); /*COM_STMT_CLOSE*/
            writer.write(statementId);
            writer.finishPacket();
        } catch (IOException e) {
            throw new QueryException(e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            lock.unlock();
        }
    }

    public void setHostFailedWithoutProxy() {
        hostFailed = true;
        close();
    }

    public UrlParser getUrlParser() {
        return urlParser;
    }

    @Override
    public boolean getAutocommit() {
        lock.lock();
        try {
            return ((serverStatus & ServerStatus.AUTOCOMMIT) != 0);
        } finally {
            lock.unlock();
        }

    }

    public boolean isMasterConnection() {
        return ParameterConstant.TYPE_MASTER.equals(currentHost.type);
    }

    public boolean mustBeMasterConnection() {
        return true;
    }

    @Override
    public boolean noBackslashEscapes() {
        return ((serverStatus & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    }

    /**
     * Connect without proxy. (use basic failover implementation)
     *
     * @throws QueryException exception
     */
    public void connectWithoutProxy() throws QueryException {
        if (!isClosed()) {
            close();
        }
        Random rand = new Random();
        List<HostAddress> addrs = urlParser.getHostAddresses();
        List<HostAddress> hosts = new LinkedList<>(addrs);

        // There could be several addresses given in the URL spec, try all of them, and throw exception if all hosts
        // fail.
        while (!hosts.isEmpty()) {
            if (urlParser.getHaMode().equals(HaMode.LOADBALANCE)) {
                currentHost = hosts.get(rand.nextInt(hosts.size()));
            } else {
                currentHost = hosts.get(0);
            }
            hosts.remove(currentHost);
            try {
                connect(currentHost.host, currentHost.port);
                return;
            } catch (IOException e) {
                if (hosts.isEmpty()) {
                    throw new QueryException("Could not connect to " + HostAddress.toString(addrs)
                            + " : " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            }
        }
    }

    public boolean shouldReconnectWithoutProxy() {
        return (!((serverStatus & ServerStatus.IN_TRANSACTION) != 0) && hostFailed && urlParser.getOptions().autoReconnect);
    }

    @Override
    public boolean inTransaction() {
        lock.lock();
        try {
            return ((serverStatus & ServerStatus.IN_TRANSACTION) != 0);
        } finally {
            lock.unlock();
        }
    }

    private void setDatatypeMappingFlags() {
        datatypeMappingFlags = 0;
        if (urlParser.getOptions().tinyInt1isBit) {
            datatypeMappingFlags |= MariaDbValueObject.TINYINT1_IS_BIT;
        }
        if (urlParser.getOptions().yearIsDateType) {
            datatypeMappingFlags |= MariaDbValueObject.YEAR_IS_DATE_TYPE;
        }
    }

    @Override
    public Options getOptions() {
        return urlParser.getOptions();
    }

    void skip() throws IOException, QueryException {
        if (activeResult != null) {
            activeResult.close();
        }

        while (moreResults) {
            getMoreResults(true);
        }

    }

    @Override
    public boolean hasMoreResults() {
        return moreResults;
    }

    public void closeExplicit() {
        this.explicitClosed = true;
        close();
    }

    /**
     * Rollback transaction.
     */
    public void rollback() {
        lock.lock();
        try {
            if (inTransaction()) {
                executeQuery(new MariaDbQuery("ROLLBACK"));
            }
        } catch (Exception e) {
            /* eat exception */
        } finally {
            lock.unlock();
        }
    }

    /**
     * Is the connection closed.
     *
     * @return true if the connection is closed
     */
    @Override
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
    private SelectQueryResult createQueryResult(final ResultSetPacket packet, boolean streaming, boolean binaryProtocol)
            throws IOException, QueryException {

        StreamingSelectResult streamingResult = StreamingSelectResult.createStreamingSelectResult(packet, packetFetcher, this, binaryProtocol);
        if (streaming) {
            return streamingResult;
        }

        return CachedSelectResult.createCachedSelectResult(streamingResult);
    }

    @Override
    public void setCatalog(final String database) throws QueryException {
        lock.lock();
//        if (log.isTraceEnabled()) log.trace("Selecting db " + database);
        final ChangeDbPacket packet = new ChangeDbPacket(database);
        try {
            packet.send(writer);
            final ByteBuffer byteBuffer = packetFetcher.getReusableBuffer();
            ResultPacket rs = ResultPacketFactory.createResultPacket(byteBuffer);
            if (rs.getResultType() == ResultPacket.ResultType.ERROR) {
                throw new QueryException("Could not select database '" + database + "' : " + ((ErrorPacket) rs).getMessage());
            }
            this.database = database;
        } catch (IOException e) {
            throw new QueryException("Could not select database '" + database + "' :" + e.getMessage(),
                    -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String getServerVersion() {
        return version;
    }

    @Override
    public boolean getReadonly() {
        return readOnly;
    }

    @Override
    public void setReadonly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public HostAddress getHostAddress() {
        return currentHost;
    }

    public void setHostAddress(HostAddress host) {
        this.currentHost = host;
        this.readOnly = ParameterConstant.TYPE_SLAVE.equals(this.currentHost.type);
    }

    @Override
    public String getHost() {
        return currentHost.host;
    }

    public FailoverProxy getProxy() {
        return proxy;
    }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }

    @Override
    public int getPort() {
        return currentHost.port;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public boolean ping() throws QueryException {
        lock.lock();
        try {
            final MariaDbPingPacket pingPacket = new MariaDbPingPacket();
            try {
                pingPacket.send(writer);
//                if (log.isTraceEnabled())log.trace("Sent ping packet");
                ByteBuffer byteBuffer = packetFetcher.getReusableBuffer();
                return ResultPacketFactory.createResultPacket(byteBuffer).getResultType() == ResultPacket.ResultType.OK;
            } catch (IOException e) {
                throw new QueryException("Could not ping: " + e.getMessage(),
                        -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                        e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public QueryResult executeQuery(Query query) throws QueryException {
        return executeQuery(query, false);
    }

    /**
     * Execute query.
     *
     * @param query the query to execute
     * @param streaming is streaming flag
     * @return queryResult
     * @throws QueryException exception
     */
    @Override
    public QueryResult executeQuery(final Query query, boolean streaming) throws QueryException {
        query.validate();
        this.moreResults = false;
        final StreamedQueryPacket packet = new StreamedQueryPacket(query);
        return executeQuery(query, packet, streaming);
    }

    /**
     * Execute list of queries.
     * This method is used when using text batch statement and using rewriting (allowMultiQueries || rewriteBatchedStatements).
     * queries will be send to server according to max_allowed_packet size.
     *
     * @param queries list of queryes
     * @param streaming is streaming flag
     * @param isRewritable is rewritable flag
     * @param rewriteOffset rewrite offset
     * @return queryresult
     * @throws QueryException exception
     */
    public QueryResult executeQuery(List<Query> queries, boolean streaming, boolean isRewritable, int rewriteOffset) throws QueryException {
        for (Query query : queries) {
            query.validate();
        }
        this.moreResults = false;
        QueryResult result = null;

        do {
            final StreamedQueryPacket packet = new StreamedQueryPacket(queries, isRewritable, rewriteOffset);
            int queriesSend = sendQuery(packet);
            if (result == null) {
                result = result(queries, streaming);
            } else {
                result.addResult(result(queries, streaming));
            }

            if (queries.size() == queriesSend) {
                return result;
            } else {
                queries = queries.subList(queriesSend, queries.size());
            }
        } while (queries.size() > 0 );

        return result;
    }


    private QueryResult executeQuery(Object queriesObj, StreamedQueryPacket packet, boolean streaming) throws QueryException {
        sendQuery(packet);
        return result(queriesObj, streaming);
    }

    private int sendQuery(StreamedQueryPacket packet)  throws QueryException {
        try {
            return packet.send(writer);
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    private QueryResult result(Object queriesObj, boolean streaming) throws QueryException {
        try {
            return getResult(queriesObj, streaming, false);
        } catch (QueryException qex) {
            if (qex.getCause() instanceof SocketTimeoutException) {
                throw new QueryException("Connection timed out", -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), qex);
            }
            throw qex;
        }
    }



    @Override
    public QueryResult getResult(Object queriesObj, boolean streaming, boolean binaryProtocol) throws QueryException {
        RawPacket rawPacket = null;
        ResultPacket resultPacket;
        try {
            rawPacket = packetFetcher.getReusableRawPacket();
            resultPacket = ResultPacketFactory.createResultPacket(rawPacket.getByteBuffer());

            if (resultPacket.getResultType() == ResultPacket.ResultType.LOCALINFILE) {
                // Server request the local file (LOCAL DATA LOCAL INFILE)
                // We do accept general URLs, too. If the localInfileStream is
                // set, use that.

                InputStream is;
                if (localInfileInputStream == null) {
                    if (!getUrlParser().getOptions().allowLocalInfile) {

                        writer.writeEmptyPacket(rawPacket.getPacketSeq() + 1);
                        throw new QueryException(
                                "Usage of LOCAL INFILE is disabled. To use it enable it via the connection property allowLocalInfile=true",
                                -1,
                                ExceptionMapper.SqlStates.FEATURE_NOT_SUPPORTED.getSqlState());
                    }
                    LocalInfilePacket localInfilePacket = (LocalInfilePacket) resultPacket;
//                    if (log.isTraceEnabled()) log.trace("sending local file " + localInfilePacket.getFileName());
                    String localInfile = localInfilePacket.getFileName();

                    try {
                        URL url = new URL(localInfile);
                        is = url.openStream();
                    } catch (IOException ioe) {
                        try {
                            is = new FileInputStream(localInfile);
                        } catch (FileNotFoundException f) {
                            writer.writeEmptyPacket(rawPacket.getPacketSeq() + 1);
                            rawPacket = packetFetcher.getReusableRawPacket();
                            ResultPacketFactory.createResultPacket(rawPacket.getByteBuffer());
                            throw new QueryException("Could not send file : " + f.getMessage(), -1, "22000", f);
                        }
                    }
                } else {
                    is = localInfileInputStream;
                    localInfileInputStream = null;
                }

                writer.sendFile(is, rawPacket.getPacketSeq() + 1);
                is.close();
                rawPacket = packetFetcher.getReusableRawPacket();
                resultPacket = ResultPacketFactory.createResultPacket(rawPacket.getByteBuffer());
            }
        } catch (SocketTimeoutException ste) {
            this.close();
            throw new QueryException("Could not read resultset: " + ste.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), ste);
        } catch (IOException e) {
            try {
                if (writer != null) {
                    writer.writeEmptyPacket(rawPacket.getPacketSeq() + 1);
                    rawPacket = packetFetcher.getReusableRawPacket();
                    ResultPacketFactory.createResultPacket(rawPacket.getByteBuffer());
                }
            } catch (IOException ee) {
            }
            throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        switch (resultPacket.getResultType()) {
            case ERROR:
                this.moreResults = false;
                this.hasWarnings = false;
                ErrorPacket ep = (ErrorPacket) resultPacket;
//                if (dQueries != null && dQueries instanceof List && ((List)dQueries).size() == 1) {
//                    log.warn("Could not execute query " + ((List)dQueries).get(0) + ": " + ((ErrorPacket) resultPacket).getMessage());
//                } else if (dQueries != null && dQueries instanceof String) {
//                    log.warn("Could not execute query " + dQueries + ": " + ((ErrorPacket) resultPacket).getMessage());
//                } else {
//                    log.warn("Got error from server: " + ((ErrorPacket) resultPacket).getMessage());
//                }
                throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());

            case OK:
                final OkPacket okpacket = (OkPacket) resultPacket;
                serverStatus = okpacket.getServerStatus();
                this.moreResults = ((serverStatus & ServerStatus.MORE_RESULTS_EXISTS) != 0);
                this.hasWarnings = (okpacket.getWarnings() > 0);
                final QueryResult updateResult = new UpdateResult(okpacket.getAffectedRows(),
                        okpacket.getWarnings(),
                        okpacket.getMessage(),
                        okpacket.getInsertId());
//                if (log.isTraceEnabled()) log.trace("OK, " + okpacket.getAffectedRows());
                return updateResult;
            case RESULTSET:
                this.hasWarnings = false;
                ResultSetPacket resultSetPacket = (ResultSetPacket) resultPacket;
                try {
                    return this.createQueryResult(resultSetPacket, streaming, binaryProtocol);
                } catch (IOException e) {

                    throw new QueryException("Could not read result set: " + e.getMessage(),
                            -1,
                            ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                            e);
                }
            default:
//                log.error("Could not parse result..." + resultPacket.getResultType());
                throw new QueryException("Could not parse result", (short) -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState());
        }

    }

    private SelectQueryResult executeSingleInternalQuery(Query query) throws QueryException {
        try {
            writer.startPacket(0);
            writer.write(0x03);
            query.writeTo(writer);
            writer.finishPacket();
            ByteBuffer buffer = packetFetcher.getReusableBuffer();
            ResultSetPacket resultSetPacket = (ResultSetPacket) ResultPacketFactory.createResultPacket(buffer);
            try {

                long fieldCount = resultSetPacket.getFieldCount();
                ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];

                for (int i = 0; i < fieldCount; i++) {
                    packetFetcher.skipNextPacket();
                    try {
                        ci[i] = new ColumnInformation(MariaDbType.STRING);
                    } catch (Exception e) {
                        throw new QueryException("Error when trying to parse field packet : " + e + ",packet content (hex) = "
                                + MariaDbProtocol.hexdump(buffer, 0), 0, "HY000", e);
                    }
                }
                ByteBuffer bufferEof = packetFetcher.getReusableBuffer();
                if (!ReadUtil.eofIsNext(bufferEof)) {
                    throw new QueryException("Packets out of order when reading field packets, expected was EOF packet. "
                            + "Packet contents (hex) = " + MariaDbProtocol.hexdump(bufferEof, 0));
                }
                return new StreamingSelectResult(ci, this, packetFetcher, false);
            } catch (IOException e) {
                throw new QueryException("Could not read result set: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }


    /**
     * Execute queries.
     *
     * @param queries queries list
     * @param streaming is streaming flag
     * @param isRewritable is rewritable flag
     * @param rewriteOffset rewriteoffset
     * @return queryResult
     * @throws QueryException exception
     */
    public QueryResult executeBatch(final List<Query> queries, boolean streaming, boolean isRewritable, int rewriteOffset) throws QueryException {
        for (Query query : queries) {
            query.validate();
        }

        this.moreResults = false;
        final StreamedQueryPacket packet = new StreamedQueryPacket(queries, isRewritable, rewriteOffset);
        try {
            packet.send(writer);
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        try {
            return getResult(queries, streaming, false);
        } catch (QueryException qex) {
            if (qex.getCause() instanceof SocketTimeoutException) {
                throw new QueryException("Connection timed out", -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), qex);
            } else {
                throw qex;
            }
        }
    }

    @Override
    public QueryResult executePreparedQueryAfterFailover(String sql, ParameterHolder[] parameters, PrepareResult oldPrepareResult,
                                                         MariaDbType[] parameterTypeHeader, boolean isStreaming) throws QueryException {
        PrepareResult prepareResult = prepare(sql);
        QueryResult queryResult = executePreparedQuery(sql, parameters, prepareResult, parameterTypeHeader, isStreaming);
        queryResult.setFailureObject(prepareResult);
        return queryResult;
    }

    @Override
    public QueryResult executePreparedQuery(String sql, ParameterHolder[] parameters, PrepareResult prepareResult, MariaDbType[] parameterTypeHeader,
                                            boolean isStreaming) throws QueryException {
        this.moreResults = false;
        try {
            int parameterCount = parameters.length;
            //send binary data in a separate packet
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].isLongData()) {
                    SendPrepareParameterPacket sendPrepareParameterPacket = new SendPrepareParameterPacket(i, (LongDataParameterHolder) parameters[i],
                            prepareResult.statementId, charset);
                    sendPrepareParameterPacket.send(writer);
                }
            }
            //send execute query
            SendExecutePrepareStatementPacket packet = new SendExecutePrepareStatementPacket(prepareResult.statementId, parameters,
                    parameterCount, parameterTypeHeader);
            packet.send(writer);

        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        try {
            return getResult(sql, isStreaming, true);
        } catch (QueryException qex) {
            if (qex.getCause() instanceof SocketTimeoutException) {
                throw new QueryException("Connection timed out", -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), qex);
            } else {
                throw qex;
            }
        }
    }

    @Override
    public void releasePrepareStatement(String sql, int statementId) throws QueryException {
//        if (log.isDebugEnabled()) log.debug("Closing prepared statement "+statementId);
        lock.lock();
        try {
            if (urlParser.getOptions().cachePrepStmts) {
                if (prepareStatementCache.containsKey(sql)) {
                    PrepareResult pr = prepareStatementCache.get(sql);
                    pr.removeUse();
                    if (!pr.hasToBeClose()) {
//                        log.debug("closing aborded, prepared statement used in another statement");
                        return;
                    }
                    prepareStatementCache.remove(sql);
                }
            }
            final SendClosePrepareStatementPacket packet = new SendClosePrepareStatementPacket(statementId);
            try {
                packet.send(writer);
            } catch (IOException e) {
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancels the current query - clones the current protocol and executes a query using the new connection.
     *
     * @throws QueryException never thrown
     * @throws IOException if Host is not responding
     */
    @Override
    public void cancelCurrentQuery() throws QueryException, IOException {
        MariaDbProtocol copiedProtocol = new MariaDbProtocol(urlParser, new ReentrantLock());
        copiedProtocol.setHostAddress(getHostAddress());
        copiedProtocol.connect();
        //no lock, because there is already a query running that possessed the lock.
        copiedProtocol.executeQuery(new MariaDbQuery("KILL QUERY " + serverThreadId));
        copiedProtocol.close();
    }

    @Override
    public QueryResult getMoreResults(boolean streaming) throws QueryException {
        if (!moreResults) {
            return null;
        }
        return getResult(null, streaming, (activeResult != null) ? activeResult.isBinaryProtocol() : false);
    }

    @Override
    public boolean hasUnreadData() {
        lock.lock();
        try {
            return (activeResult != null);
        } finally {
            lock.unlock();
        }

    }

    /**
     * Set max row retuen by a statement.
     *
     * @param max row number max value
     */
    public void setInternalMaxRows(int max) {
        if (maxRows != max) {
            maxRows = max;
        }
    }

    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws QueryException {
        if (maxRows != max) {
            if (max == 0) {
                executeQuery(new MariaDbQuery("set @@SQL_SELECT_LIMIT=DEFAULT"));
            } else {
                executeQuery(new MariaDbQuery("set @@SQL_SELECT_LIMIT=" + max));
            }
            maxRows = max;
        }
    }

    void parseVersion() {
        String[] versionArray = version.split("[^0-9]");
        if (versionArray.length > 0) {
            majorVersion = Integer.parseInt(versionArray[0]);
        }
        if (versionArray.length > 1) {
            minorVersion = Integer.parseInt(versionArray[1]);
        }
        if (versionArray.length > 2) {
            patchVersion = Integer.parseInt(versionArray[2]);
        }
    }

    @Override
    public int getMajorServerVersion() {
        return majorVersion;

    }

    @Override
    public int getMinorServerVersion() {
        return minorVersion;
    }

    @Override
    public boolean versionGreaterOrEqual(int major, int minor, int patch) {
        if (this.majorVersion > major) {
            return true;
        }
        if (this.majorVersion < major) {
            return false;
        }
        /*
         * Major versions are equal, compare minor versions
        */
        if (this.minorVersion > minor) {
            return true;
        }
        if (this.minorVersion < minor) {
            return false;
        }

        //Minor versions are equal, compare patch version.
        if (this.patchVersion > patch) {
            return true;
        }
        if (this.patchVersion < patch) {
            return false;
        }

        // Patch versions are equal => versions are equal.
        return true;
    }

    @Override
    public void setLocalInfileInputStream(InputStream inputStream) {
        this.localInfileInputStream = inputStream;
    }

    /**
     * Returns the connection timeout in milliseconds.
     *
     * @return the connection timeout in milliseconds.
     * @throws SocketException if there is an error in the underlying protocol, such as a TCP error.
     */
    @Override
    public int getTimeout() throws SocketException {
        return this.socket.getSoTimeout();
    }

    /**
     * Sets the connection timeout.
     *
     * @param timeout the timeout, in milliseconds
     * @throws SocketException if there is an error in the underlying protocol, such as a TCP error.
     */
    @Override
    public void setTimeout(int timeout) throws SocketException {
        lock.lock();
        try {
            this.getOptions().socketTimeout = timeout;
            this.socket.setSoTimeout(timeout);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean getPinGlobalTxToPhysicalConnection() {
        return this.urlParser.getOptions().pinGlobalTxToPhysicalConnection;
    }

    /**
     * Set transaction isolation.
     *
     * @param level transaction level.
     * @throws QueryException if transaction level is unknown
     */
    public void setTransactionIsolation(final int level) throws QueryException {
        lock.lock();
        try {
            String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
            switch (level) {
                case Connection.TRANSACTION_READ_UNCOMMITTED:
                    query += " READ UNCOMMITTED";
                    break;
                case Connection.TRANSACTION_READ_COMMITTED:
                    query += " READ COMMITTED";
                    break;
                case Connection.TRANSACTION_REPEATABLE_READ:
                    query += " REPEATABLE READ";
                    break;
                case Connection.TRANSACTION_SERIALIZABLE:
                    query += " SERIALIZABLE";
                    break;
                default:
                    throw new QueryException("Unsupported transaction isolation level");
            }
            executeQuery(new MariaDbQuery(query));
            transactionIsolationLevel = level;
        } finally {
            lock.unlock();
        }
    }

    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    /**
     * Has warnings.
     *
     * @return true if as warnings.
     */
    public boolean hasWarnings() {
        lock.lock();
        try {
            return hasWarnings;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Is connected.
     *
     * @return true if connected
     */
    public boolean isConnected() {
        lock.lock();
        try {
            return connected;
        } finally {
            lock.unlock();
        }
    }

    public long getServerThreadId() {
        return serverThreadId;
    }

    public int getDatatypeMappingFlags() {
        return datatypeMappingFlags;
    }

    /**
     * Close active result.
     */
    public void closeIfActiveResult() {
        if (activeResult != null) {
            activeResult.close();
        }
    }

    public boolean isExplicitClosed() {
        return explicitClosed;
    }

    public PrepareStatementCache prepareStatementCache() {
        return prepareStatementCache;
    }

    public Calendar getCalendar() {
        return cal;
    }
}
