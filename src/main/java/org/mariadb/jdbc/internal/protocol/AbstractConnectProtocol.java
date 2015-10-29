package org.mariadb.jdbc.internal.protocol;

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
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.MyX509TrustManager;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.packet.send.SendOldPasswordAuthPacket;
import org.mariadb.jdbc.internal.queryresults.AbstractQueryResult;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.util.*;
import org.mariadb.jdbc.internal.util.buffer.ReadUtil;
import org.mariadb.jdbc.internal.packet.read.RawPacket;
import org.mariadb.jdbc.internal.packet.read.ReadInitialConnectPacket;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.read.ReadResultPacketFactory;
import org.mariadb.jdbc.internal.query.MariaDbQuery;
import org.mariadb.jdbc.internal.query.Query;
import org.mariadb.jdbc.internal.queryresults.SelectQueryResult;
import org.mariadb.jdbc.internal.queryresults.StreamingSelectResult;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.constant.MariaDbCharset;
import org.mariadb.jdbc.internal.util.constant.ParameterConstant;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.queryresults.ValueObject;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.packet.send.SendClosePacket;
import org.mariadb.jdbc.internal.packet.send.SendHandshakeResponsePacket;
import org.mariadb.jdbc.internal.packet.send.SendSslConnectionRequestPacket;
import org.mariadb.jdbc.internal.stream.DecompressInputStream;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractConnectProtocol implements Protocol {
    private final String username;
    private final String password;
    private boolean hostFailed;
    private String version;
    private int majorVersion;
    private int minorVersion;
    private int patchVersion;
    private byte serverLanguage;
    private Map<String, String> serverData;
    private Calendar cal;

    protected final ReentrantLock lock;
    protected final UrlParser urlParser;
    protected Socket socket;
    protected PacketOutputStream writer;
    protected boolean readOnly = false;
    protected ReadPacketFetcher packetFetcher;
    protected HostAddress currentHost;
    protected FailoverProxy proxy;
    protected boolean connected = false;
    protected boolean explicitClosed = false;
    protected String database;
    protected long serverThreadId;
    protected MariaDbCharset charset;
    protected PrepareStatementCache prepareStatementCache;

    public boolean moreResults = false;
    public boolean hasWarnings = false;
    public StreamingSelectResult activeResult = null;
    public int dataTypeMappingFlags;
    public short serverStatus;

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL infos
     * @param lock the lock for thread synchronisation
     */

    public AbstractConnectProtocol(final UrlParser urlParser, final ReentrantLock lock) {
        this.lock = lock;
        this.urlParser = urlParser;
        this.database = (urlParser.getDatabase() == null ? "" : urlParser.getDatabase());
        this.username = (urlParser.getUsername() == null ? "" : urlParser.getUsername());
        this.password = (urlParser.getPassword() == null ? "" : urlParser.getPassword());
        if (urlParser.getOptions().cachePrepStmts) {
            prepareStatementCache = PrepareStatementCache.newInstance(urlParser.getOptions().prepStmtCacheSize);
        }
        setDataTypeMappingFlags();
    }


    private void skip() throws IOException, QueryException {
        if (activeResult != null) {
            activeResult.close();
        }

        while (moreResults) {
            getMoreResults(true);
        }

    }

    public abstract AbstractQueryResult getMoreResults(boolean streaming) throws QueryException;

    /**
     * Closes socket and stream readers/writers Attempts graceful shutdown.
     */
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
            if (urlParser.getOptions().cachePrepStmts) {
                prepareStatementCache.clear();
            }
            close(packetFetcher, writer, socket);
        } catch (Exception e) {
            // socket is closed, so it is ok to ignore exception
        } finally {
            this.connected = false;

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
     * InitializeSocketOption.
     */
    private void initializeSocketOption() {
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

        handleConnectionPhases();

        if (urlParser.getOptions().useCompression) {
            writer.setUseCompression(true);
            packetFetcher = new ReadPacketFetcher(new DecompressInputStream(socket.getInputStream()));
        }

        setSessionOptions();
        loadServerData();
        writer.setMaxAllowedPacket(Integer.parseInt(serverData.get("max_allowed_packet")));

        createDatabaseIfNotExist();
        loadCalendar();


        activeResult = null;
        moreResults = false;
        hasWarnings = false;
        connected = true;
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
        // In JDBC, connection must start in autocommit mode.
        if ((serverStatus & ServerStatus.AUTOCOMMIT) == 0) {
            executeQuery(new MariaDbQuery("set autocommit=1"));
        }
        if (urlParser.getOptions().sessionVariables != null) {
            executeQuery(new MariaDbQuery("set session " + urlParser.getOptions().sessionVariables));
        }
    }

    private void handleConnectionPhases() throws QueryException {
        InputStream reader = null;
        try {
            reader = new BufferedInputStream(socket.getInputStream(), 32768);
            packetFetcher = new ReadPacketFetcher(reader);
            writer = new PacketOutputStream(socket.getOutputStream());

            final ReadInitialConnectPacket greetingPacket = new ReadInitialConnectPacket(packetFetcher);
            this.serverThreadId = greetingPacket.getServerThreadId();
            this.serverLanguage = greetingPacket.getServerLanguage();
            this.charset = CharsetUtils.getServerCharset(serverLanguage);
            this.version = greetingPacket.getServerVersion();
            parseVersion();
            int clientCapabilities = initializeClientCapabilities();

            byte packetSeq = 1;
            if (urlParser.getOptions().useSsl && (greetingPacket.getServerCapabilities() & MariaDbServerCapabilities.SSL) != 0) {
                clientCapabilities |= MariaDbServerCapabilities.SSL;
                SendSslConnectionRequestPacket amcap = new SendSslConnectionRequestPacket(clientCapabilities);
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
                packetFetcher = new ReadPacketFetcher(reader);

                packetSeq++;
            } else if (urlParser.getOptions().useSsl) {
                throw new QueryException("Trying to connect with ssl, but ssl not enabled in the server");
            }

            authentication(clientCapabilities, greetingPacket.getSeed(), packetSeq);

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

    private void authentication(int clientCapabilities, byte[] seed, byte packetSeq) throws QueryException, IOException {
        final SendHandshakeResponsePacket cap = new SendHandshakeResponsePacket(this.username,
                this.password,
                database,
                clientCapabilities,
                decideLanguage(),
                seed,
                packetSeq);
        cap.send(writer);
        RawPacket rp = packetFetcher.getRawPacket();

        if ((rp.getByteBuffer().get(0) & 0xFF) == 0xFE) {   // Server asking for old format password
            final SendOldPasswordAuthPacket oldPassPacket = new SendOldPasswordAuthPacket(
                    this.password, Utils.copyWithLength(seed,8), rp.getPacketSeq() + 1);
            oldPassPacket.send(writer);
            rp = packetFetcher.getRawPacket();
        }

        AbstractResultPacket resultPacket = ReadResultPacketFactory.createResultPacket(rp.getByteBuffer());
        if (resultPacket.getResultType() == AbstractResultPacket.ResultType.ERROR) {
            ErrorPacket errorPacket = (ErrorPacket) resultPacket;
            throw new QueryException("Could not connect: " + errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        serverStatus = ((OkPacket) resultPacket).getServerStatus();
    }

    private int initializeClientCapabilities() {
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
        return capabilities;
    }

    /**
     * If createDB is true, then just try to create the database and to use it.
     * @throws QueryException if connection failed
     */
    private void createDatabaseIfNotExist() throws QueryException {
        if (checkIfMaster()) {
            if (urlParser.getOptions().createDatabaseIfNotExist) {
                // Try to create the database if it does not exist
                String quotedDb = MariaDbConnection.quoteIdentifier(this.database);
                executeQuery(new MariaDbQuery("CREATE DATABASE IF NOT EXISTS " + quotedDb));
                executeQuery(new MariaDbQuery("USE " + quotedDb));
            }
        }
    }

    private void loadCalendar() throws QueryException {
        String timeZone = null;
        if (urlParser.getOptions().serverTimezone != null) {
            timeZone = urlParser.getOptions().serverTimezone;
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
            if (!urlParser.getOptions().useLegacyDatetimeCode) {
                if (urlParser.getOptions().serverTimezone != null) {
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

    /**
     * Check that next read packet is a End-of-file packet.
     * @throws QueryException if not a End-of-file packet
     * @throws IOException if connection error occur
     */
    public void readEofPacket() throws QueryException, IOException {
        AbstractResultPacket resultPacket = ReadResultPacketFactory.createResultPacket(packetFetcher);
        switch (resultPacket.getResultType()) {
            case EOF:
                EndOfFilePacket eof = (EndOfFilePacket) resultPacket;
                this.hasWarnings = eof.getWarningCount() > 0;
                this.serverStatus = eof.getStatusFlags();
                break;
            case ERROR:
                ErrorPacket ep = (ErrorPacket) resultPacket;
                throw new QueryException("Could not connect: " + ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
            default:
                throw new QueryException("Unexpected stream type " + resultPacket.getResultType()
                        + "insted of EOF");
        }
    }

    public void setHostFailedWithoutProxy() {
        hostFailed = true;
        close();
    }

    public UrlParser getUrlParser() {
        return urlParser;
    }

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


    private SelectQueryResult executeSingleInternalQuery(Query query) throws QueryException {
        try {
            writer.startPacket(0);
            writer.write(0x03);
            query.writeTo(writer);
            writer.finishPacket();
            ResultSetPacket resultSetPacket = (ResultSetPacket) ReadResultPacketFactory.createResultPacket(packetFetcher);
            try {
                long fieldCount = resultSetPacket.getFieldCount();
                ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];

                for (int i = 0; i < fieldCount; i++) {
                    packetFetcher.skipNextPacket();
                    ci[i] = new ColumnInformation(MariaDbType.STRING);
                }

                ByteBuffer bufferEof = packetFetcher.getReusableBuffer();
                if (!ReadUtil.eofIsNext(bufferEof)) {
                    throw new QueryException("Packets out of order when reading field packets, expected was EOF stream. "
                            + "Packet contents (hex) = " + MasterProtocol.hexdump(bufferEof, 0));
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
        return this.urlParser.getOptions().pinGlobalTxToPhysicalConnection;
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
        if (urlParser.getOptions().tinyInt1isBit) {
            dataTypeMappingFlags |= ValueObject.TINYINT1_IS_BIT;
        }
        if (urlParser.getOptions().yearIsDateType) {
            dataTypeMappingFlags |= ValueObject.YEAR_IS_DATE_TYPE;
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
        return urlParser.getOptions();
    }

    public abstract AbstractQueryResult executeQuery(Query query) throws QueryException;

}
