package org.mariadb.jdbc.internal.protocol;

/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015 Avaya Inc.
Copyright (c) 2015-2016 MariaDB Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson

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

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.MariaDbServerCapabilities;
import org.mariadb.jdbc.internal.MyX509TrustManager;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.packet.send.*;
import org.mariadb.jdbc.internal.protocol.authentication.AuthenticationProviderHolder;
import org.mariadb.jdbc.internal.queryresults.ExecutionResult;
import org.mariadb.jdbc.internal.queryresults.SingleExecutionResult;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.stream.MariaDbBufferedInputStream;
import org.mariadb.jdbc.internal.stream.MariaDbInputStream;
import org.mariadb.jdbc.internal.util.*;
import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.packet.read.ReadInitialConnectPacket;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.Packet;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.constant.ParameterConstant;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.stream.DecompressInputStream;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractConnectProtocol implements Protocol {
    private final String username;
    private final String password;
    private boolean hostFailed;
    private String version;
    protected boolean isMariaServer;
    private int majorVersion;
    private int minorVersion;
    private int patchVersion;
    private Map<String, String> serverData;
    private Calendar cal;

    protected final ReentrantLock lock;
    protected final UrlParser urlParser;
    protected final Options options;
    protected Socket socket;
    protected PacketOutputStream writer;
    protected boolean readOnly = false;
    protected ReadPacketFetcher packetFetcher;
    protected HostAddress currentHost;
    protected FailoverProxy proxy;
    protected volatile boolean connected = false;
    protected boolean explicitClosed = false;
    protected String database;
    protected long serverThreadId;
    protected ServerPrepareStatementCache serverPrepareStatementCache;
    protected boolean moreResults = false;

    public boolean moreResultsTypeBinary = false;
    public boolean hasWarnings = false;
    public MariaSelectResultSet activeStreamingResult = null;
    public int dataTypeMappingFlags;
    public short serverStatus;
    public boolean serverAcceptComMulti = false;

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL infos
     * @param lock the lock for thread synchronisation
     */

    public AbstractConnectProtocol(final UrlParser urlParser, final ReentrantLock lock) {
        this.lock = lock;
        this.urlParser = urlParser;
        this.options = this.urlParser.getOptions();
        this.database = (urlParser.getDatabase() == null ? "" : urlParser.getDatabase());
        this.username = (urlParser.getUsername() == null ? "" : urlParser.getUsername());
        this.password = (urlParser.getPassword() == null ? "" : urlParser.getPassword());
        if (options.cachePrepStmts) {
            serverPrepareStatementCache = ServerPrepareStatementCache.newInstance(options.prepStmtCacheSize, this);
        }

        setDataTypeMappingFlags();
    }

    /**
     * Skip packets not read that are not needed.
     * Packets are read according to needs.
     * If some data have not been read before next execution, skip it.
     *
     * @throws QueryException exception
     */
    public void skip() throws SQLException, QueryException {
        if (activeStreamingResult != null) {
            activeStreamingResult.close();
        }

        while (moreResults) {
            SingleExecutionResult execution = new SingleExecutionResult(null, 0, true, false);
            getMoreResults(execution);
        }
    }

    public abstract void getMoreResults(ExecutionResult executionResult) throws QueryException;

    public void setMoreResults(boolean moreResults, boolean isBinary) {
        this.moreResults = moreResults;
        this.moreResultsTypeBinary = isBinary;
    }

    /**
     * Closes socket and stream readers/writers Attempts graceful shutdown.
     */
    public void close() {
        if (lock != null) {
            lock.lock();
        }
        this.connected = false;
        try {
            /* If a streaming result set is open, close it.*/
            skip();
        } catch (Exception e) {
            /* eat exception */
        }
        try {
            if (options.cachePrepStmts) {
                serverPrepareStatementCache.clear();
            }
            close(packetFetcher, writer, socket);
        } catch (Exception e) {
            // socket is closed, so it is ok to ignore exception
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    protected static void close(ReadPacketFetcher fetcher, PacketOutputStream packetOutputStream, Socket socket) throws QueryException {
        SendClosePacket closePacket = new SendClosePacket();
        try {
            try {
                closePacket.send(packetOutputStream);
                socket.shutdownOutput();
                socket.setSoTimeout(3);
                InputStream is = socket.getInputStream();
                while (is.read() != -1) {
                }
            } catch (Throwable t) {
                //eat exception
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
                //socket closed, if any error, so not throwing error
            }
        }
    }

    private SSLSocketFactory getSslSocketFactory() throws QueryException {
        if (!options.trustServerCertificate
                && options.serverSslCert == null
                && options.trustCertificateKeyStoreUrl == null
                && options.clientCertificateKeyStoreUrl == null) {
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");

            X509TrustManager[] trustManager = null;
            if (options.trustServerCertificate || options.serverSslCert != null
                    || options.trustCertificateKeyStoreUrl != null) {
                trustManager = new X509TrustManager[]{new MyX509TrustManager(options)};
            }

            KeyManager[] keyManager = null;
            String clientCertKeystoreUrl = options.clientCertificateKeyStoreUrl;
            if (clientCertKeystoreUrl != null && !clientCertKeystoreUrl.isEmpty()) {
                keyManager = loadClientCerts(clientCertKeystoreUrl, options.clientCertificateKeyStorePassword);
            }

            sslContext.init(keyManager, trustManager, null);
            return sslContext.getSocketFactory();
        } catch (Exception e) {
            throw new QueryException(e.getMessage(), 0, "HY000", e);
        }

    }

    private KeyManager[] loadClientCerts(String keystoreUrl, String keystorePassword) throws Exception {
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        InputStream inStream = null;
        try {
            char[] certKeystorePassword = keystorePassword == null ? null : keystorePassword.toCharArray();
            inStream = new URL(keystoreUrl).openStream();
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(inStream, certKeystorePassword);
            keyManagerFactory.init(ks, certKeystorePassword);
        } finally {
            if (inStream != null) {
                inStream.close();
            }
        }
        return keyManagerFactory.getKeyManagers();
    }

    /**
     * InitializeSocketOption.
     */
    private void initializeSocketOption() {
        try {
            if (options.tcpNoDelay) {
                socket.setTcpNoDelay(options.tcpNoDelay);
            } else {
                socket.setTcpNoDelay(true);
            }

            if (options.tcpKeepAlive) {
                socket.setKeepAlive(true);
            }
            if (options.tcpRcvBuf != null) {
                socket.setReceiveBufferSize(options.tcpRcvBuf);
            }
            if (options.tcpSndBuf != null) {
                socket.setSendBufferSize(options.tcpSndBuf);
            }
            if (options.tcpAbortiveClose) {
                socket.setSoLinger(true, 0);
            }
        } catch (Exception e) {
//            if (log.isDebugEnabled())log.debug("Failed to set socket option: " + e.getLocalizedMessage());
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
     * Connect the client and perform handshake.
     *
     * @throws QueryException : handshake error, e.g wrong user or password
     * @throws IOException : connection error (host/port not available)
     */
    private void connect(String host, int port) throws QueryException, IOException {

        socket = Utils.createSocket(urlParser, host);
        initializeSocketOption();

        // Bind the socket to a particular interface if the connection property
        // localSocketAddress has been defined.
        if (options.localSocketAddress != null) {
            InetSocketAddress localAddress = new InetSocketAddress(options.localSocketAddress, 0);
            socket.bind(localAddress);
        }

        if (!socket.isConnected()) {
            InetSocketAddress sockAddr = new InetSocketAddress(host, port);
            if (options.connectTimeout != null) {
                socket.connect(sockAddr, options.connectTimeout);
            } else {
                socket.connect(sockAddr);
            }
        }

        // Extract socketTimeout URL parameter
        if (options.socketTimeout != null) {
            socket.setSoTimeout(options.socketTimeout);
        }

        handleConnectionPhases();

        if (options.useCompression) {
            writer.setUseCompression(true);
            packetFetcher = new ReadPacketFetcher(new DecompressInputStream(socket.getInputStream()));
        }
        connected = true;

        writer.forceCleanupBuffer();

        loadServerData();
        setSessionOptions();
        writer.setMaxAllowedPacket(Integer.parseInt(serverData.get("max_allowed_packet")));

        createDatabaseIfNotExist();
        loadCalendar();


        activeStreamingResult = null;
        moreResults = false;
        hasWarnings = false;
        hostFailed = false;
    }

    /**
     * Is the connection closed.
     *
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return !this.connected;
    }

    private void setSessionOptions()  throws QueryException {
        // In JDBC, connection must start in autocommit mode
        // [CONJ-269] we cannot rely on serverStatus & ServerStatus.AUTOCOMMIT before this command to avoid this command.
        // if autocommit=0 is set on server configuration, DB always send Autocommit on serverStatus flag
        // after setting autocommit, we can rely on serverStatus value
        String sessionOption = "autocommit=1";

        if (options.jdbcCompliantTruncation) {
            if (serverData.get("sql_mode") == null || "".equals(serverData.get("sql_mode"))) {
                sessionOption += ",sql_mode='STRICT_TRANS_TABLES'";
            } else {
                if (!serverData.get("sql_mode").contains("STRICT_TRANS_TABLES")) {
                    sessionOption += ",sql_mode='" + serverData.get("sql_mode") + ",STRICT_TRANS_TABLES'";
                }
            }
        }
        if (options.sessionVariables != null) {
            sessionOption += "," + options.sessionVariables;
        }
        executeQuery("set session " + sessionOption);
    }

    private void handleConnectionPhases() throws QueryException {
        MariaDbInputStream reader = null;
        try {
            reader = new MariaDbBufferedInputStream(socket.getInputStream(), 16384);
            packetFetcher = new ReadPacketFetcher(reader);
            writer = new PacketOutputStream(socket.getOutputStream(), options.profileSql || options.slowQueryThresholdNanos != null);

            final ReadInitialConnectPacket greetingPacket = new ReadInitialConnectPacket(packetFetcher);
            this.serverThreadId = greetingPacket.getServerThreadId();
            this.version = greetingPacket.getServerVersion();
            this.isMariaServer = this.version.indexOf("MariaDB") != -1;
            this.serverAcceptComMulti = (greetingPacket.getServerCapabilities() & MariaDbServerCapabilities.MARIADB_CLIENT_COM_MULTI) != 0;
            byte exchangeCharset = decideLanguage(greetingPacket.getServerLanguage());
            parseVersion();
            long clientCapabilities = initializeClientCapabilities();

            byte packetSeq = 1;
            if (options.useSsl && (greetingPacket.getServerCapabilities() & MariaDbServerCapabilities.SSL) != 0) {
                clientCapabilities |= MariaDbServerCapabilities.SSL;
                SendSslConnectionRequestPacket amcap = new SendSslConnectionRequestPacket(clientCapabilities, exchangeCharset);
                amcap.send(writer);

                SSLSocketFactory sslSocketFactory = getSslSocketFactory();
                SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(socket,
                        socket.getInetAddress().getHostAddress(), socket.getPort(), true);

                enabledSslProtocolSuites(sslSocket);
                enabledSslCipherSuites(sslSocket);

                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();
                socket = sslSocket;
                writer = new PacketOutputStream(socket.getOutputStream(), options.profileSql || options.slowQueryThresholdNanos != null);
                reader = new MariaDbBufferedInputStream(socket.getInputStream(), 16384);
                packetFetcher = new ReadPacketFetcher(reader);

                packetSeq++;
            } else if (options.useSsl) {
                throw new QueryException("Trying to connect with ssl, but ssl not enabled in the server");
            }

            authentication(exchangeCharset, clientCapabilities, greetingPacket.getSeed(), packetSeq,
                    greetingPacket.getPluginName(), greetingPacket.getServerCapabilities());

        } catch (IOException e) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ee) {
                    //eat exception
                }
            }
            throw new QueryException("Could not connect to " + currentHost.host + ":" + currentHost.port + ": " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    private void authentication(byte exchangeCharset, long clientCapabilities, byte[] seed, byte packetSeq, String plugin, long serverCapabilities)
            throws QueryException, IOException {
        final SendHandshakeResponsePacket cap = new SendHandshakeResponsePacket(this.username,
                this.password,
                database,
                clientCapabilities,
                exchangeCharset,
                seed,
                packetSeq,
                plugin,
                options.connectionAttributes,
                serverThreadId);
        cap.send(writer);
        Buffer buffer = packetFetcher.getPacket();

        if ((buffer.getByteAt(0) & 0xFF) == 0xFE) {
            InterfaceAuthSwitchSendResponsePacket interfaceSendPacket;
            if ((serverCapabilities & MariaDbServerCapabilities.PLUGIN_AUTH) != 0) {
                //AuthSwitchRequest packet.
                buffer.readByte();
                plugin = buffer.readString(Charset.forName("ASCII"));
                byte[] authData = buffer.readRawBytes(buffer.remaining());

                //Authentication according to plugin.
                //see AuthenticationProviderHolder for implement other plugin
                interfaceSendPacket = AuthenticationProviderHolder.getAuthenticationProvider()
                        .processAuthPlugin(packetFetcher, plugin, password, authData, packetFetcher.getLastPacketSeq() + 1);
            } else {
                interfaceSendPacket = new SendOldPasswordAuthPacket(this.password, Utils.copyWithLength(seed, 8),
                        packetFetcher.getLastPacketSeq() + 1);
            }
            interfaceSendPacket.send(writer);
            interfaceSendPacket.handleResultPacket(packetFetcher);
        } else {
            if (buffer.getByteAt(0) == Packet.ERROR) {
                ErrorPacket errorPacket = new ErrorPacket(buffer);
                throw new QueryException("Could not connect: " + errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
            }
            serverStatus = new OkPacket(buffer).getServerStatus();
        }

    }

    private long initializeClientCapabilities() {
        long capabilities =
                //MariaDbServerCapabilities.CLIENT_MYSQL
                        MariaDbServerCapabilities.IGNORE_SPACE
                        | MariaDbServerCapabilities.CLIENT_PROTOCOL_41
                        | MariaDbServerCapabilities.TRANSACTIONS
                        | MariaDbServerCapabilities.SECURE_CONNECTION
                        | MariaDbServerCapabilities.LOCAL_FILES
                        | MariaDbServerCapabilities.MULTI_RESULTS
                        | MariaDbServerCapabilities.PS_MULTI_RESULTS
                        | MariaDbServerCapabilities.FOUND_ROWS
                        | MariaDbServerCapabilities.PLUGIN_AUTH
                        | MariaDbServerCapabilities.CONNECT_ATTRS
                        | MariaDbServerCapabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA
                        | MariaDbServerCapabilities.MARIADB_CLIENT_COM_MULTI;

        if (options.allowMultiQueries || (options.rewriteBatchedStatements)) {
            capabilities |= MariaDbServerCapabilities.MULTI_STATEMENTS;
        }

        if (options.useCompression) {
            capabilities |= MariaDbServerCapabilities.COMPRESS;
        }
        if (options.interactiveClient) {
            capabilities |= MariaDbServerCapabilities.CLIENT_INTERACTIVE;
        }

        // If a database is given, but createDatabaseIfNotExist is not defined or is false,
        // then just try to connect to the given database
        if (database != null && !options.createDatabaseIfNotExist) {
            capabilities |= MariaDbServerCapabilities.CONNECT_WITH_DB;
        }
        return capabilities;
    }

    /**
     * If createDB is true, then just try to create the database and to use it.
     * @throws QueryException if connection failed
     */
    private void createDatabaseIfNotExist() throws QueryException {
        if (checkIfMaster() && options.createDatabaseIfNotExist) {
            // Try to create the database if it does not exist
            String quotedDb = MariaDbConnection.quoteIdentifier(this.database);
            executeQuery("CREATE DATABASE IF NOT EXISTS " + quotedDb);
            executeQuery("USE " + quotedDb);
        }
    }

    private void loadCalendar() throws QueryException {
        String timeZone = null;
        if (options.serverTimezone != null) {
            timeZone = options.serverTimezone;
        }

        if (timeZone == null) {
            timeZone = getServerData("time_zone");
            if ("SYSTEM".equals(timeZone)) {
                timeZone = getServerData("system_time_zone");
            }
        }
        //handle custom timezone id
        if (timeZone != null && timeZone.length() >= 2
                && (timeZone.startsWith("+") || timeZone.startsWith("-"))
                && Character.isDigit(timeZone.charAt(1))) {
            timeZone = "GMT" + timeZone;
        }
        try {
            TimeZone tz = Utils.getTimeZone(timeZone);
            cal = Calendar.getInstance(tz);
        } catch (SQLException e) {
            cal = null;
            if (!options.useLegacyDatetimeCode) {
                if (options.serverTimezone != null) {
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
        SingleExecutionResult qr = new SingleExecutionResult(null, 0, true, false);
        try {
            executeQuery(true, qr, "SHOW VARIABLES WHERE Variable_name in ("
                    + "'max_allowed_packet', "
                    + "'system_time_zone', "
                    + "'time_zone', "
                    + "'sql_mode'"
                    + ")", ResultSet.TYPE_FORWARD_ONLY);
            MariaSelectResultSet resultSet = qr.getResultSet();
            while (resultSet.next()) {
                serverData.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException sqle) {
            throw new QueryException("could not load system variables", -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), sqle);
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

    private byte decideLanguage(byte serverLanguage) {
        //force UTF8mb4 if possible, UTF8 if not.
        byte result = (isServerLanguageUtf8mb4(serverLanguage) ? serverLanguage : 33);
        return result;
    }

    /**
     * Check that next read packet is a End-of-file packet.
     * @throws QueryException if not a End-of-file packet
     * @throws IOException if connection error occur
     */
    public void readEofPacket() throws QueryException, IOException {
        Buffer buffer = packetFetcher.getReusableBuffer();
        switch (buffer.getByteAt(0)) {
            case (byte) 0xfe: //EOF
                EndOfFilePacket eof = new EndOfFilePacket(buffer);
                this.hasWarnings = eof.getWarningCount() > 0;
                this.serverStatus = eof.getStatusFlags();
                break;
            case (byte) 0xff: //ERROR
                ErrorPacket ep = new ErrorPacket(buffer);
                throw new QueryException("Could not connect: " + ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
            default:
                throw new QueryException("Unexpected stream type " + buffer.getByteAt(0)
                        + " instead of EOF");
        }
    }

    public void setHostFailedWithoutProxy() {
        hostFailed = true;
        close();
    }

    public UrlParser getUrlParser() {
        return urlParser;
    }

    /**
     * Indicate if current protocol is a master protocol.
     * @return is master flag
     */
    public boolean isMasterConnection() {
        return ParameterConstant.TYPE_MASTER.equals(currentHost.type);
    }

    public boolean mustBeMasterConnection() {
        return true;
    }

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

        //CONJ-293 : handle name-pipe without host
        if (hosts.isEmpty() && options.pipe != null) {
            try {
                connect(null, 0);
                return;
            } catch (IOException e) {
                if (hosts.isEmpty()) {
                    throw new QueryException("Could not connect to named pipe '" + options.pipe + "' : "
                            + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            }
        }

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

    public String getServerVersion() {
        return version;
    }

    public boolean getReadonly() {
        return readOnly;
    }

    public void setReadonly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    public HostAddress getHostAddress() {
        return currentHost;
    }

    public void setHostAddress(HostAddress host) {
        this.currentHost = host;
        this.readOnly = ParameterConstant.TYPE_SLAVE.equals(this.currentHost.type);
    }

    public String getHost() {
        return currentHost.host;
    }

    public FailoverProxy getProxy() {
        return proxy;
    }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }

    public int getPort() {
        return currentHost.port;
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

    private void parseVersion() {
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

    public int getMajorServerVersion() {
        return majorVersion;

    }

    public int getMinorServerVersion() {
        return minorVersion;
    }

    /**
     * Return possible protocols : values of option enabledSslProtocolSuites is set, or default to "TLSv1,TLSv1.1".
     *   MariaDB versions &ge; 10.0.15 and &ge; 5.5.41 supports TLSv1.2 if compiled with openSSL (default).
     *   MySQL community versions &ge; 5.7.10 is compile with yaSSL, so max TLS is TLSv1.1.
     *
     * @param sslSocket current sslSocket
     * @throws QueryException if protocol isn't a supported protocol
     */
    protected void enabledSslProtocolSuites(SSLSocket sslSocket) throws QueryException {
        if (options.enabledSslProtocolSuites == null) {
            sslSocket.setEnabledProtocols(new String[] {"TLSv1", "TLSv1.1"});
        } else {
            List<String> possibleProtocols = Arrays.asList(sslSocket.getSupportedProtocols());
            String[] protocols = options.enabledSslProtocolSuites.split("[,;\\s]+");
            for (String protocol : protocols) {
                if (!possibleProtocols.contains(protocol)) {
                    throw new QueryException("Unsupported SSL protocol '" + protocol + "'. Supported protocols : "
                            + possibleProtocols.toString().replace("[", "").replace("]", ""));
                }
            }
            sslSocket.setEnabledProtocols(protocols);
        }
    }

    /**
     * Set ssl socket cipher according to options.
     *
     * @param sslSocket current ssl socket
     * @throws QueryException if a cipher isn't known
     */
    protected void enabledSslCipherSuites(SSLSocket sslSocket) throws QueryException {
        if (options.enabledSslCipherSuites != null) {
            List<String> possibleCiphers = Arrays.asList(sslSocket.getEnabledCipherSuites());
            String[] ciphers = options.enabledSslCipherSuites.split("[,;\\s]+");
            for (String cipher : ciphers) {
                if (!possibleCiphers.contains(cipher)) {
                    throw new QueryException("Unsupported SSL cipher '" + cipher + "'. Supported ciphers : "
                            + possibleCiphers.toString().replace("[", "").replace("]", ""));
                }
            }
            sslSocket.setEnabledCipherSuites(ciphers);
        }
    }

    /**
     * Utility method to check if database version is greater than parameters.
     * @param major major version
     * @param minor minor version
     * @param patch patch version
     * @return true if version is greater than parameters
     */
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

    public boolean getPinGlobalTxToPhysicalConnection() {
        return this.options.pinGlobalTxToPhysicalConnection;
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

    private void setDataTypeMappingFlags() {
        dataTypeMappingFlags = 0;
        if (options.tinyInt1isBit) {
            dataTypeMappingFlags |= MariaSelectResultSet.TINYINT1_IS_BIT;
        }
        if (options.yearIsDateType) {
            dataTypeMappingFlags |= MariaSelectResultSet.YEAR_IS_DATE_TYPE;
        }
    }

    public long getServerThreadId() {
        return serverThreadId;
    }

    public int getDataTypeMappingFlags() {
        return dataTypeMappingFlags;
    }

    public boolean isExplicitClosed() {
        return explicitClosed;
    }

    public Calendar getCalendar() {
        return cal;
    }

    public Options getOptions() {
        return options;
    }

    public void setHasWarnings(boolean hasWarnings) {
        this.hasWarnings = hasWarnings;
    }

    public MariaSelectResultSet getActiveStreamingResult() {
        return activeStreamingResult;
    }

    public void setActiveStreamingResult(MariaSelectResultSet activeStreamingResult) {
        this.activeStreamingResult = activeStreamingResult;
    }

    @Override
    public ReentrantLock getLock() {
        return lock;
    }

    @Override
    public boolean hasMoreResults() {
        return moreResults;
    }

    public ServerPrepareStatementCache prepareStatementCache() {
        return serverPrepareStatementCache;
    }

    public abstract void executeQuery(final String sql) throws QueryException;

    public boolean isServerComMulti() {
        return serverAcceptComMulti;
    }

    public void releaseWriterBuffer() {
        writer.releaseBuffer();
    }

    public ByteBuffer getWriter() {
        return writer.buffer;
    }
}
