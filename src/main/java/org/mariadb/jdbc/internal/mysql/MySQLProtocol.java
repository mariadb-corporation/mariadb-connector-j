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
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.MySQLConnection;
import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.*;
import org.mariadb.jdbc.internal.common.packet.*;
import org.mariadb.jdbc.internal.common.packet.buffer.ReadUtil;
import org.mariadb.jdbc.internal.common.packet.buffer.Reader;
import org.mariadb.jdbc.internal.common.packet.commands.ClosePacket;
import org.mariadb.jdbc.internal.common.packet.commands.SelectDBPacket;
import org.mariadb.jdbc.internal.common.packet.commands.StreamedQueryPacket;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.query.Query;
import org.mariadb.jdbc.internal.common.queryresults.*;
import org.mariadb.jdbc.internal.mysql.listener.Listener;
import org.mariadb.jdbc.internal.mysql.listener.tools.SearchFilter;
import org.mariadb.jdbc.internal.mysql.packet.MySQLGreetingReadPacket;
import org.mariadb.jdbc.internal.mysql.packet.commands.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import javax.net.ssl.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class MyX509TrustManager implements X509TrustManager {
    String serverCertFile;
    X509TrustManager  trustManager;

    public MyX509TrustManager(Options options) throws Exception{
        boolean trustServerCertificate  = options.trustServerCertificate;
        if (trustServerCertificate)
            return;

        serverCertFile =  options.serverSslCert;
        InputStream inStream;

        if (serverCertFile.startsWith("-----BEGIN CERTIFICATE-----")) {
            inStream = new ByteArrayInputStream(serverCertFile.getBytes());
        } else if (serverCertFile.startsWith("classpath:")) {
            // Load it from a classpath relative file
            String classpathFile = serverCertFile.substring("classpath:".length());
            inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathFile);
        } else {
            inStream = new FileInputStream(serverCertFile);
        }

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> caList = cf.generateCertificates(inStream);
        inStream.close();
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            // Note: KeyStore requires it be loaded even if you don't load anything into it:
            ks.load(null);
        } catch (Exception e) {

        }
        for(Certificate ca : caList) {
            ks.setCertificateEntry(UUID.randomUUID().toString(), ca);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        for(TrustManager tm : tmf.getTrustManagers()) {
        	if (tm instanceof X509TrustManager) {
      	        trustManager = (X509TrustManager) tm;
      		    break;
            }
        }
        if (trustManager == null) {
            throw new RuntimeException("No X509TrustManager found");
        }
    }
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

    }

    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        if (trustManager == null) {
            return;
        }
        trustManager.checkServerTrusted(x509Certificates, s);
    }

    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }
}

public class MySQLProtocol implements Protocol {

    private final static Logger log = LoggerFactory.getLogger(MySQLProtocol.class);
    protected final ReentrantReadWriteLock lock;
    private boolean connected = false;
    private boolean explicitClosed = false;
    protected Socket socket;
    protected PacketOutputStream writer;
    private  String version;
    protected boolean readOnly = false;
    private String database;
    private final String username;
    private final String password;
    private int maxRows;  /* max rows returned by a statement */
    protected SyncPacketFetcher packetFetcher;
    private  long serverThreadId;
    public boolean moreResults = false;
    public boolean hasWarnings = false;
    public StreamingSelectResult activeResult= null;
    public int datatypeMappingFlags;
    public short serverStatus;
    protected final JDBCUrl jdbcUrl;
    protected HostAddress currentHost;
    protected FailoverProxy proxy;
    private int majorVersion;
    private int minorVersion;
    private int patchVersion;
    private int maxAllowedPacket;
    private byte serverLanguage;
    private int transactionIsolationLevel=0;
    boolean hostFailed;

    /* =========================== HA  parameters ========================================= */

    private InputStream localInfileInputStream;

    private SSLSocketFactory getSSLSocketFactory(boolean trustServerCertificate)  throws QueryException
    {
        if (jdbcUrl.getOptions().trustServerCertificate
                && jdbcUrl.getOptions().serverSslCert == null) {
            return (SSLSocketFactory)SSLSocketFactory.getDefault();
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            X509TrustManager[] m = {new MyX509TrustManager(jdbcUrl.getOptions())};
            sslContext.init(null, m ,null);
            return sslContext.getSocketFactory();
        } catch (Exception e) {
            throw new QueryException(e.getMessage(),0, "HY000", e);
        }

    }
    /**
     * Get a protocol instance
     * @param jdbcUrl connection URL infos
     * @param lock the lock for thread synchronisation
     */

    public MySQLProtocol(final JDBCUrl jdbcUrl, final ReentrantReadWriteLock lock) {
        this.lock = lock;
        this.jdbcUrl = jdbcUrl;
        this.database = (jdbcUrl.getDatabase() == null ? "" : jdbcUrl.getDatabase());
        this.username = (jdbcUrl.getUsername() == null ? "" : jdbcUrl.getUsername());
        this.password = (jdbcUrl.getPassword() == null ? "" : jdbcUrl.getPassword());

        setDatatypeMappingFlags();
    }

    /**
     * Connect the client and perform handshake
     *
     * @throws QueryException  : handshake error, e.g wrong user or password
     * @throws IOException : connection error (host/port not available)
     */
    private void connect(String host, int port) throws QueryException, IOException{

        SocketFactory socketFactory = null;
        String socketFactoryName = jdbcUrl.getOptions().socketFactory;
        if (socketFactoryName != null) {
            try {
                socketFactory = (SocketFactory) (Class.forName(socketFactoryName).newInstance());
            } catch (Exception sfex){
                log.debug("Failed to create socket factory " + socketFactoryName);
                socketFactory = SocketFactory.getDefault();
            }
        }  else {
            socketFactory = SocketFactory.getDefault();
        }

        // Create socket with timeout if required
        if (jdbcUrl.getOptions().pipe != null) {
            socket = new org.mariadb.jdbc.internal.mysql.NamedPipeSocket(host, jdbcUrl.getOptions().pipe);
        } else if(jdbcUrl.getOptions().localSocket != null){
            try {
                socket = new org.mariadb.jdbc.internal.mysql.UnixDomainSocket(jdbcUrl.getOptions().localSocket);
            } catch( RuntimeException re) {
               //  could be e.g library loading error
                throw new IOException(re.getMessage(),re.getCause());
            }
        } else if(jdbcUrl.getOptions().sharedMemory != null) {
            try {
                socket = new SharedMemorySocket(jdbcUrl.getOptions().sharedMemory);
            } catch( RuntimeException re) {
                //  could be e.g library loading error
                throw new IOException(re.getMessage(),re.getCause());
            }
        } else {
            socket = socketFactory.createSocket();
        }

        try {
            if (jdbcUrl.getOptions().tcpNoDelay) socket.setTcpNoDelay(true);
            if (jdbcUrl.getOptions().tcpKeepAlive) socket.setKeepAlive(true);
            if (jdbcUrl.getOptions().tcpRcvBuf != null) socket.setReceiveBufferSize(jdbcUrl.getOptions().tcpRcvBuf);
            if (jdbcUrl.getOptions().tcpSndBuf != null) socket.setSendBufferSize(jdbcUrl.getOptions().tcpSndBuf);
            if (jdbcUrl.getOptions().tcpAbortiveClose) socket.setSoLinger(true, 0);
        } catch (Exception e) {
            if (log.isDebugEnabled())log.debug("Failed to set socket option: " + e.getLocalizedMessage());
        }

        // Bind the socket to a particular interface if the connection property
        // localSocketAddress has been defined.
        if (jdbcUrl.getOptions().localSocketAddress != null) {
            InetSocketAddress localAddress = new InetSocketAddress(jdbcUrl.getOptions().localSocketAddress, 0);
            socket.bind(localAddress);
        }

        if (!socket.isConnected()) {
            InetSocketAddress sockAddr = new InetSocketAddress(host, port);
            if (jdbcUrl.getOptions().connectTimeout != null) {
                socket.connect(sockAddr, jdbcUrl.getOptions().connectTimeout);
            } else {
                socket.connect(sockAddr);
            }
        }

        // Extract socketTimeout URL parameter
        if (jdbcUrl.getOptions().socketTimeout != null) socket.setSoTimeout(jdbcUrl.getOptions().socketTimeout);

        try {
            InputStream reader;
            reader = new BufferedInputStream(socket.getInputStream(), 32768);
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
            this.serverLanguage = greetingPacket.getServerLanguage();

            this.version = greetingPacket.getServerVersion();
            parseVersion();
            byte packetSeq = 1;
            int capabilities =
                    MySQLServerCapabilities.LONG_PASSWORD |
                            MySQLServerCapabilities.IGNORE_SPACE |
                            MySQLServerCapabilities.CLIENT_PROTOCOL_41|
                            MySQLServerCapabilities.TRANSACTIONS|
                            MySQLServerCapabilities.SECURE_CONNECTION|
                            MySQLServerCapabilities.LOCAL_FILES|
                            MySQLServerCapabilities.MULTI_RESULTS|
                            MySQLServerCapabilities.FOUND_ROWS;


            if(jdbcUrl.getOptions().allowMultiQueries || (jdbcUrl.getOptions().rewriteBatchedStatements)) {
                capabilities |= MySQLServerCapabilities.MULTI_STATEMENTS;
            }

            if(jdbcUrl.getOptions().useCompression) capabilities |= MySQLServerCapabilities.COMPRESS;
            if(jdbcUrl.getOptions().interactiveClient) capabilities |= MySQLServerCapabilities.CLIENT_INTERACTIVE;

            // If a database is given, but createDatabaseIfNotExist is not defined or is false,
            // then just try to connect to the given database
            if (database != null && !jdbcUrl.getOptions().createDatabaseIfNotExist)
                capabilities |= MySQLServerCapabilities.CONNECT_WITH_DB;

            if(jdbcUrl.getOptions().useSSL &&
                    (greetingPacket.getServerCapabilities() & MySQLServerCapabilities.SSL) != 0 ) {
                capabilities |= MySQLServerCapabilities.SSL;
                AbbreviatedMySQLClientAuthPacket amcap = new AbbreviatedMySQLClientAuthPacket(capabilities);
                amcap.send(writer);

                SSLSocketFactory f = getSSLSocketFactory(jdbcUrl.getOptions().trustServerCertificate);
                SSLSocket sslSocket = (SSLSocket)f.createSocket(socket,
                        socket.getInetAddress().getHostAddress(),  socket.getPort(), true);

                sslSocket.setEnabledProtocols(new String [] {"TLSv1"});
                sslSocket.setUseClientMode(true);
                sslSocket.startHandshake();
                socket = sslSocket;
                writer = new PacketOutputStream(socket.getOutputStream());
                reader = new BufferedInputStream(socket.getInputStream(), 32768);
                packetFetcher = new SyncPacketFetcher(reader);

                packetSeq++;
            } else if(jdbcUrl.getOptions().useSSL){
                throw new QueryException("Trying to connect with ssl, but ssl not enabled in the server");
            }

            final MySQLClientAuthPacket cap = new MySQLClientAuthPacket(this.username,
                    this.password,
                    database,
                    capabilities,
                    decideLanguage(),
                    greetingPacket.getSeed(),
                    packetSeq);
            cap.send(writer);

            RawPacket rp = packetFetcher.getRawPacket();

            if ((rp.getByteBuffer().get(0) & 0xFF) == 0xFE) {   // Server asking for old format password
                final MySQLClientOldPasswordAuthPacket oldPassPacket = new MySQLClientOldPasswordAuthPacket(
                        this.password, Utils.copyWithLength(greetingPacket.getSeed(),
                        8), rp.getPacketSeq() + 1);
                oldPassPacket.send(writer);

                rp = packetFetcher.getRawPacket();
            }

            checkErrorPacket(rp);
            ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rp);
            OKPacket ok = (OKPacket)resultPacket;
            serverStatus = ok.getServerStatus();

            if (jdbcUrl.getOptions().useCompression) {
                writer = new PacketOutputStream(new CompressOutputStream(socket.getOutputStream()));
                packetFetcher = new SyncPacketFetcher(new DecompressInputStream(socket.getInputStream()));
            }

            // In JDBC, connection must start in autocommit mode.
            if ((serverStatus & ServerStatus.AUTOCOMMIT) == 0) {
                executeQuery(new MySQLQuery("set autocommit=1"));
            }
           SelectQueryResult qr = null;
           try {
               qr = (SelectQueryResult) executeQuery(new MySQLQuery("show variables like 'max_allowed_packet'"));
               if (qr.next()) {
                   setMaxAllowedPacket(qr.getValueObject(1).getInt());
               }
           } finally {
               if (qr != null)qr.close();
           }

           if (jdbcUrl.getOptions().sessionVariables != null) executeQuery(new MySQLQuery("set session " + jdbcUrl.getOptions().sessionVariables));

            // At this point, the driver is connected to the database, if createDB is true,
            // then just try to create the database and to use it
            if (checkIfMaster()) {
                if (jdbcUrl.getOptions().createDatabaseIfNotExist) {
                    // Try to create the database if it does not exist
                    String quotedDB = MySQLConnection.quoteIdentifier(this.database);
                    executeQuery(new MySQLQuery("CREATE DATABASE IF NOT EXISTS " + quotedDB));
                    executeQuery(new MySQLQuery("USE " + quotedDB));
                }
            }

            activeResult = null;
            moreResults = false;
            hasWarnings = false;
            connected = true;
            hostFailed = false;
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + host + ":" +
                    port + ": " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }

    }

    public boolean checkIfMaster() throws QueryException  {
        return isMasterConnection();
    }

    private boolean isServerLanguageUTF8MB4(byte serverLanguage) {
        Byte[] utf8mb4Languages = {
                (byte)45,(byte)46,(byte)224,(byte)225,(byte)226,(byte)227,(byte)228,
                (byte)229,(byte)230,(byte)231,(byte)232,(byte)233,(byte)234,(byte)235,
                (byte)236,(byte)237,(byte)238,(byte)239,(byte)240,(byte)241,(byte)242,
                (byte)243,(byte)245
        };
        return Arrays.asList(utf8mb4Languages).contains(serverLanguage);
    }
    private byte decideLanguage() {
        byte result = (byte) (isServerLanguageUTF8MB4(this.serverLanguage) ? this.serverLanguage : 33);
        return result;
    }

    void checkErrorPacket(RawPacket rp) throws QueryException{
        if (rp.getByteBuffer().get(0) == -1) {
            ErrorPacket ep = new ErrorPacket(rp);
            String message = ep.getMessage();
            throw new QueryException("Could not connect: " + message, ep.getErrorNumber(), ep.getSqlState());
        }
    }


    void readEOFPacket() throws QueryException, IOException {
        RawPacket rp = packetFetcher.getRawPacket();
        checkErrorPacket(rp);
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rp);
        if (resultPacket.getResultType() != ResultPacket.ResultType.EOF) {
            throw new QueryException("Unexpected packet type " + resultPacket.getResultType()  +
                    "insted of EOF");
        }
        EOFPacket eof = (EOFPacket)resultPacket;
        this.hasWarnings = eof.getWarningCount() > 0;
        this.serverStatus = eof.getStatusFlags();
    }

    void readOKPacket()  throws QueryException, IOException  {
        RawPacket rp = packetFetcher.getRawPacket();
        checkErrorPacket(rp);
        ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rp);
        if (resultPacket.getResultType() != ResultPacket.ResultType.OK) {
            throw new QueryException("Unexpected packet type " + resultPacket.getResultType()  +
                    "insted of OK");
        }
        OKPacket ok = (OKPacket)resultPacket;
        this.hasWarnings = ok.getWarnings() > 0;
        this.serverStatus = ok.getServerStatus();
    }

    public class PrepareResult {
        public int statementId;
        public MySQLColumnInformation[] columns;
        public MySQLColumnInformation[] parameters;
        public PrepareResult(int statementId, MySQLColumnInformation[] columns,  MySQLColumnInformation parameters[]) {
            this.statementId = statementId;
            this.columns = columns;
            this.parameters = parameters;
        }
    }

    @Override
    public  PrepareResult prepare(String sql) throws QueryException {
        try {
            writer.startPacket(0);
            writer.write(0x16);
            writer.write(sql.getBytes("UTF8"));
            writer.finishPacket();

            RawPacket rp  = packetFetcher.getRawPacket();
            checkErrorPacket(rp);
            byte b = rp.getByteBuffer().get(0);
            if (b == 0) {
                /* Prepared Statement OK */
                Reader r = new Reader(rp);
                r.readByte(); /* skip field count */
                int statementId = r.readInt();
                int numColumns = r.readShort();
                int numParams = r.readShort();
                r.readByte(); // reserved
                this.hasWarnings = r.readShort() > 0;
                MySQLColumnInformation[] params = new MySQLColumnInformation[numParams];
                if (numParams > 0) {
                    for (int i = 0; i < numParams; i++) {
                        params[i] = new MySQLColumnInformation(packetFetcher.getRawPacket());
                    }
                    readEOFPacket();
                }
                MySQLColumnInformation[] columns = new MySQLColumnInformation[numColumns];
                if (numColumns > 0) {
                    for (int i = 0; i < numColumns; i++) {
                        columns[i] = new MySQLColumnInformation(packetFetcher.getRawPacket());
                    }
                    readEOFPacket();
                }

                return new PrepareResult(statementId,columns,params);
            } else {
                throw new QueryException("Unexpected packet returned by server, first byte " + b);
            }
        } catch (IOException e) {
            throw new QueryException(e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }
    }

    @Override
    public void closePreparedStatement(int statementId) throws QueryException {
        lock.writeLock().lock();
        try {
            writer.startPacket(0);
            writer.write(0x19); /*COM_STMT_CLOSE*/
            writer.write(statementId);
            writer.finishPacket();
        } catch(IOException e) {
            throw new QueryException(e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setHostFailedWithoutProxy() {
        hostFailed = true;
        close();
    }

    public JDBCUrl getJdbcUrl() {
        return jdbcUrl;
    }

    public static MySQLProtocol getNewProtocol(FailoverProxy proxy, JDBCUrl jdbcUrl) {
        MySQLProtocol newProtocol = new MySQLProtocol(jdbcUrl, proxy.lock);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }

    @Override
    public boolean getAutocommit() {
        lock.readLock().lock();
        try {
            return ((serverStatus & ServerStatus.AUTOCOMMIT) != 0);
        } finally {
            lock.readLock().unlock();
        }

    }

    public boolean isMasterConnection() {
        return ParameterConstant.TYPE_MASTER.equals(currentHost.type);
    }

    public boolean mustBeMasterConnection() { return true; }

    @Override
    public boolean noBackslashEscapes() {
        lock.readLock().lock();
        try {
            return ((serverStatus & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void connect() throws QueryException {
        if (!isClosed()) {
            close();
        }
        try {
            connect(currentHost.host, currentHost.port);
            return;
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + currentHost + "." + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    public void connectWithoutProxy() throws QueryException {
        if (!isClosed()) {
            close();
        }

        List<HostAddress> addrs = jdbcUrl.getHostAddresses();

        // There could be several addresses given in the URL spec, try all of them, and throw exception if all hosts
        // fail.
        for(int i = 0; i < addrs.size(); i++) {
            currentHost = addrs.get(i);
            try {
                connect(currentHost.host, currentHost.port);
                return;
            } catch (IOException e) {
                if (i == addrs.size() - 1) {
                    throw new QueryException("Could not connect to " + HostAddress.toString(addrs) +
                            " : " + e.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            }
        }
    }
    public boolean shouldReconnectWithoutProxy() {
        return (!inTransaction() && hostFailed && jdbcUrl.getOptions().autoReconnect);
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
    public static void loop(Listener listener, final List<HostAddress> addresses, Map<HostAddress, Long> blacklist, SearchFilter searchFilter) throws QueryException {
        if (log.isDebugEnabled()) {
            log.debug("searching for master:" + searchFilter.isSearchForMaster() + " replica:" + searchFilter.isSearchForSlave() + " addresses:" + addresses);
        }

        MySQLProtocol protocol;
        List<HostAddress> loopAddresses = new LinkedList<>(addresses);
        int maxConnectionTry = listener.getRetriesAllDown();
        QueryException lastQueryException = null;
        while (!loopAddresses.isEmpty() || (!searchFilter.isUniqueLoop() && maxConnectionTry > 0)) {
            protocol = getNewProtocol(listener.getProxy(), listener.getJdbcUrl());

            if (listener.isExplicitClosed()) return;
            maxConnectionTry--;

            try {
                protocol.setHostAddress(loopAddresses.get(0));
                loopAddresses.remove(0);

                if (log.isDebugEnabled()) log.debug("trying to connect to " + protocol.getHostAddress());
                protocol.connect();
                blacklist.remove(protocol.getHostAddress());
                if (log.isDebugEnabled()) log.debug("connected to primary " + protocol.getHostAddress());
                listener.foundActiveMaster(protocol);
                return;

            } catch (QueryException e) {
                blacklist.put(protocol.getHostAddress(), System.currentTimeMillis());
                if (log.isDebugEnabled()) log.debug("Could not connect to " + protocol.getHostAddress() + " searching: " + searchFilter + " error: " + e.getMessage());
                lastQueryException = e;
            }

            //loop is set so
            if (loopAddresses.isEmpty() && !searchFilter.isUniqueLoop() && maxConnectionTry > 0) {
                loopAddresses = new LinkedList<>(addresses);
            }
        }
        if (lastQueryException != null) {
            throw new QueryException("No active connection found for master", lastQueryException.getErrorCode(), lastQueryException.getSqlState(), lastQueryException);
        }
        throw new QueryException("No active connection found for master");
    }

    @Override
    public boolean inTransaction() {
        lock.readLock().lock();
        try {
            return ((serverStatus & ServerStatus.IN_TRANSACTION) != 0);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void setDatatypeMappingFlags() {
        datatypeMappingFlags = 0;
        if (jdbcUrl.getOptions().tinyInt1isBit) datatypeMappingFlags |= MySQLValueObject.TINYINT1_IS_BIT;
        if (jdbcUrl.getOptions().yearIsDateType) datatypeMappingFlags |= MySQLValueObject.YEAR_IS_DATE_TYPE;
    }

    @Override
    public Options getOptions() {
        return jdbcUrl.getOptions();
    }

    void skip() throws IOException, QueryException{
        if (activeResult != null) {
            activeResult.close();
        }

        while (moreResults) {
            getMoreResults(true);
        }

    }

    @Override
    public boolean  hasMoreResults() {
        return moreResults;
    }


    protected static void close(PacketFetcher fetcher, PacketOutputStream packetOutputStream, Socket socket) throws QueryException {
        ClosePacket closePacket = new ClosePacket();
        try {
            try {
                closePacket.send(packetOutputStream);
                socket.shutdownOutput();
                socket.setSoTimeout(3);
                InputStream is = socket.getInputStream();
                while(is.read() != -1) {}
            } catch (Throwable t) {
            }
            packetOutputStream.close();
            fetcher.close();
        } catch (IOException e) {
            throw new QueryException("Could not close connection: " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("Could not close socket");
            }
        }
    }


    public void closeExplicit() {
        this.explicitClosed = true;
        close();
    }
    /**
     * Closes socket and stream readers/writers
     * Attempts graceful shutdown.
     */
    @Override
    public void close() {
        if (lock != null) lock.writeLock().lock();
        try {
            /* If a streaming result set is open, close it.*/
            skip();
        } catch (Exception e) {
            /* eat exception */
        }
        try {
            if (log.isTraceEnabled()) log.trace("Closing connection  " + currentHost);
            close(packetFetcher, writer, socket);
        } catch (Exception e) {
            // socket is closed, so it is ok to ignore exception
            log.debug("got exception " + e + " while closing connection");
        } finally {
            this.connected = false;

            if (lock != null) lock.writeLock().unlock();
        }
    }

    public void rollback() {
        lock.writeLock().lock();
        try {
            if (inTransaction()) executeQuery(new MySQLQuery("ROLLBACK"));
        } catch (Exception e) {
            /* eat exception */
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
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
    private SelectQueryResult createQueryResult(final ResultSetPacket packet, boolean streaming) throws IOException, QueryException {

        StreamingSelectResult streamingResult =   StreamingSelectResult.createStreamingSelectResult(packet, packetFetcher, this);
        if (streaming)
            return streamingResult;

        return CachedSelectResult.createCachedSelectResult(streamingResult);
    }

    @Override
    public void setCatalog(final String database) throws QueryException {
        lock.writeLock().lock();
        if (log.isTraceEnabled()) log.trace("Selecting db " + database);
        final SelectDBPacket packet = new SelectDBPacket(database);
        try {
            packet.send(writer);
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacket rs = ResultPacketFactory.createResultPacket(rawPacket);
            if (rs.getResultType() == ResultPacket.ResultType.ERROR) {
                throw new QueryException("Could not select database '" + database +"' : "+ ((ErrorPacket) rs).getMessage());
            }
            this.database = database;
        } catch (IOException e) {
            throw new QueryException("Could not select database '" + database +"' :"+ e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String getServerVersion() {
        return version;
    }

    @Override
    public void setReadonly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public boolean getReadonly() {
        return readOnly;
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
    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }
    public FailoverProxy getProxy() {
        return proxy;
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
        lock.writeLock().lock();
        try {
            final MySQLPingPacket pingPacket = new MySQLPingPacket();
            try {
                pingPacket.send(writer);
                if (log.isTraceEnabled())log.trace("Sent ping packet");
                final RawPacket rawPacket = packetFetcher.getRawPacket();
                return ResultPacketFactory.createResultPacket(rawPacket).getResultType() == ResultPacket.ResultType.OK;
            } catch (IOException e) {
                throw new QueryException("Could not ping: " + e.getMessage(),
                        -1,
                        SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                        e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public QueryResult executeQuery(Query dQuery)  throws QueryException {
        return executeQuery(dQuery, false);
    }

    @Override
    public QueryResult getResult(List<Query> dQueries, boolean streaming) throws QueryException {

        RawPacket rawPacket;
        ResultPacket resultPacket;
        try {
            rawPacket = packetFetcher.getRawPacket();
            resultPacket = ResultPacketFactory.createResultPacket(rawPacket);

            if (resultPacket.getResultType() == ResultPacket.ResultType.LOCALINFILE) {
                // Server request the local file (LOCAL DATA LOCAL INFILE)
                // We do accept general URLs, too. If the localInfileStream is
                // set, use that.

                InputStream is;
                if (localInfileInputStream == null) {
                    if (!getJdbcUrl().getOptions().allowLocalInfile) {
                      throw new QueryException(
                          "Usage of LOCAL INFILE is disabled. To use it enable it via the connection property allowLocalInfile=true",
                          -1,
                          SQLExceptionMapper.SQLStates.FEATURE_NOT_SUPPORTED.getSqlState());
                    }
                    LocalInfilePacket localInfilePacket = (LocalInfilePacket) resultPacket;
                    if (log.isTraceEnabled()) log.trace("sending local file " + localInfilePacket.getFileName());
                    String localInfile = localInfilePacket.getFileName();

                    try {
                        URL u = new URL(localInfile);
                        is = u.openStream();
                    } catch (IOException ioe) {
                        is = new FileInputStream(localInfile);
                    }
                } else {
                    is = localInfileInputStream;
                    localInfileInputStream = null;
                }

                writer.sendFile(is, rawPacket.getPacketSeq() + 1);
                is.close();
                rawPacket = packetFetcher.getRawPacket();
                resultPacket = ResultPacketFactory.createResultPacket(rawPacket);
            }
        } catch (SocketTimeoutException ste) {
            this.close();
            throw new QueryException("Could not read resultset: " + ste.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    ste);
        } catch (IOException e) {
            throw new QueryException("Could not read resultset: " + e.getMessage(),
                    -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }

        switch (resultPacket.getResultType()) {
            case ERROR:
                this.moreResults = false;
                this.hasWarnings = false;
                ErrorPacket ep = (ErrorPacket) resultPacket;
                if (dQueries != null && dQueries.size() == 1) {
                    log.warn("Could not execute query " + dQueries.get(0) + ": " + ((ErrorPacket) resultPacket).getMessage());
                } else {
                    log.warn("Got error from server: " + ((ErrorPacket) resultPacket).getMessage());
                }
                throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());

            case OK:
                final OKPacket okpacket = (OKPacket) resultPacket;
                serverStatus = okpacket.getServerStatus();
                this.moreResults = ((serverStatus & ServerStatus.MORE_RESULTS_EXISTS) != 0);
                this.hasWarnings = (okpacket.getWarnings() > 0);
                final QueryResult updateResult = new UpdateResult(okpacket.getAffectedRows(),
                        okpacket.getWarnings(),
                        okpacket.getMessage(),
                        okpacket.getInsertId());
                if (log.isTraceEnabled()) log.trace("OK, " + okpacket.getAffectedRows());
                return updateResult;
            case RESULTSET:
                this.hasWarnings = false;
                ResultSetPacket resultSetPacket = (ResultSetPacket) resultPacket;
                try {
                    return this.createQueryResult(resultSetPacket, streaming);
                } catch (IOException e) {

                    throw new QueryException("Could not read result set: " + e.getMessage(),
                            -1,
                            SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                            e);
                }
            default:
                log.error("Could not parse result..." + resultPacket.getResultType());
                throw new QueryException("Could not parse result", (short) -1, SQLExceptionMapper.SQLStates.INTERRUPTED_EXCEPTION.getSqlState());
        }

    }

    @Override
    public QueryResult executeQuery(final Query query, boolean streaming) throws QueryException {
        List<Query> queries = new ArrayList<Query>();
        queries.add(query);
        return executeQuery(queries, streaming, false, 0);
    }

    public QueryResult executeQuery(final List<Query> dQueries, boolean streaming, boolean isRewritable, int rewriteOffset) throws QueryException {
        for (Query query : dQueries) query.validate();

        this.moreResults = false;
        final StreamedQueryPacket packet = new StreamedQueryPacket(dQueries, isRewritable, rewriteOffset);
        try {
            packet.send(writer);
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) connect();
            throw new QueryException("Could not send query: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        try {
            return getResult(dQueries, streaming);
        } catch (QueryException qex) {
            if (qex.getCause() instanceof SocketTimeoutException) {
                throw new QueryException("Connection timed out", -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), qex);
            } else {
                throw qex;
            }
        }
    }




    private String getServerVariable(String variable) throws QueryException {
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
     *
     * @throws QueryException never thrown
     * @throws IOException if Host is not responding
     */
    @Override
    public  void cancelCurrentQuery() throws QueryException, IOException {
        MySQLProtocol copiedProtocol = new MySQLProtocol(jdbcUrl, null);
        copiedProtocol.setHostAddress(getHostAddress());
        copiedProtocol.connect();
        //no lock, because there is already a query running that possessed the lock.
        copiedProtocol.executeQuery(new MySQLQuery("KILL QUERY " + serverThreadId));
        copiedProtocol.close();
    }

    @Override
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


    @Override
    public boolean hasUnreadData() {
        lock.readLock().lock();
        try {
            return (activeResult != null);
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public void setMaxRows(int max) throws QueryException {
        if (maxRows != max) {
            if (max == 0) {
                executeQuery(new MySQLQuery("set @@SQL_SELECT_LIMIT=DEFAULT"));
            } else {
                executeQuery(new MySQLQuery("set @@SQL_SELECT_LIMIT=" + max));
            }
            maxRows = max;
        }
    }
    public void setInternalMaxRows(int max) {
        if (maxRows != max) {
            maxRows = max;
        }
    }

    public int getMaxRows() {
        return maxRows;
    }

    void parseVersion() {
        String[] a = version.split("[^0-9]");
        if (a.length > 0)
            majorVersion = Integer.parseInt(a[0]);
        if (a.length > 1)
            minorVersion = Integer.parseInt(a[1]);
        if (a.length > 2)
            patchVersion = Integer.parseInt(a[2]);
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
        if (this.majorVersion > major)
            return true;
        if (this.majorVersion < major)
            return false;
    	/*
    	 * Major versions are equal, compare minor versions
    	 */
        if (this.minorVersion > minor)
            return true;
        if (this.minorVersion < minor)
            return false;

    	/*
    	 * Minor versions are equal, compare patch version
    	 */
        if (this.patchVersion > patch)
            return true;
        if (this.patchVersion < patch)
            return false;

    	/* Patch versions are equal => versions are equal */
        return true;
    }
    @Override
    public void setLocalInfileInputStream(InputStream inputStream) {
        this.localInfileInputStream = inputStream;
    }

    public int getMaxAllowedPacket() {
        return this.maxAllowedPacket;
    }

	public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
		writer.setMaxAllowedPacket(maxAllowedPacket);
	}

	/**
	 * Sets the connection timeout.
	 * @param timeout     the timeout, in milliseconds
	 * @throws SocketException  if there is an error in the underlying protocol, such as a TCP error.
	 */
    @Override
    public void setTimeout(int timeout) throws SocketException {
        lock.writeLock().lock();
        try {
            this.getOptions().socketTimeout = timeout;
            this.socket.setSoTimeout(timeout);
        } finally {
            lock.writeLock().unlock();
        }
    }
    /**
     * Returns the connection timeout in milliseconds.
     * @return the connection timeout in milliseconds.
     * @throws SocketException  if there is an error in the underlying protocol, such as a TCP error.
     */
    @Override
    public int getTimeout() throws SocketException {
        return this.socket.getSoTimeout();
    }

    @Override
    public boolean getPinGlobalTxToPhysicalConnection() {
        return this.jdbcUrl.getOptions().pinGlobalTxToPhysicalConnection;
    }


    public void setTransactionIsolation(final int level) throws QueryException {
        lock.writeLock().lock();
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
            executeQuery(new MySQLQuery(query));
            transactionIsolationLevel = level;
        } finally {
            lock.writeLock().unlock();
        }
    }
    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    public boolean hasWarnings() {
        lock.readLock().lock();
        try {
            return hasWarnings;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isConnected() {
        lock.readLock().lock();
        try {
            return connected;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getServerThreadId(){
        return serverThreadId;
    }
    public int getDatatypeMappingFlags() {
        return datatypeMappingFlags;
    }

    public void closeIfActiveResult() {
        if (activeResult != null) activeResult.close();
    }

    public boolean isExplicitClosed() {
        return explicitClosed;
    }

}
