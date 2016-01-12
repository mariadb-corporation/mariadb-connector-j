package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.packet.send.*;
import org.mariadb.jdbc.internal.queryresults.*;
import org.mariadb.jdbc.internal.stream.MaxAllowedPacketException;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.PrepareStatementCache;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.buffer.Reader;
import org.mariadb.jdbc.internal.packet.read.RawPacket;
import org.mariadb.jdbc.internal.packet.read.ReadResultPacketFactory;
import org.mariadb.jdbc.internal.query.MariaDbQuery;
import org.mariadb.jdbc.internal.query.Query;
import org.mariadb.jdbc.internal.packet.dao.parameters.LongDataParameterHolder;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

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

public class AbstractQueryProtocol extends AbstractConnectProtocol implements Protocol {

    private int transactionIsolationLevel = 0;
    private InputStream localInfileInputStream;
    private int maxRows;  /* max rows returned by a statement */

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL infos
     * @param lock the lock for thread synchronisation
     */

    public AbstractQueryProtocol(final UrlParser urlParser, final ReentrantLock lock) {
        super(urlParser, lock);
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


    @Override
    public PrepareResult prepare(String sql) throws QueryException {
        try {
            if (urlParser.getOptions().cachePrepStmts && prepareStatementCache.containsKey(sql)) {
                PrepareResult pr = prepareStatementCache.get(sql);
                pr.addUse();
                return pr;
            }

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
                if (urlParser.getOptions().cachePrepStmts && sql != null && sql.length() < urlParser.getOptions().prepStmtCacheSqlLimit) {
                    prepareStatementCache.putIfNone(sql, prepareResult);
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

    @Override
    public boolean getAutocommit() {
        lock.lock();
        try {
            return ((serverStatus & ServerStatus.AUTOCOMMIT) != 0);
        } finally {
            lock.unlock();
        }

    }

    @Override
    public boolean inTransaction() {
        return ((serverStatus & ServerStatus.IN_TRANSACTION) != 0);
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
        final SendChangeDbPacket packet = new SendChangeDbPacket(database);
        try {
            packet.send(writer);
            final ByteBuffer byteBuffer = packetFetcher.getReusableBuffer();
            if (byteBuffer.get(0) == ReadResultPacketFactory.ERROR) {
                AbstractResultPacket rs = ReadResultPacketFactory.createResultPacket(byteBuffer);
                final ErrorPacket ep = (ErrorPacket) rs;
                throw new QueryException("Could not select database '" + database + "' : " + ep.getMessage(),
                    ep.getErrorNumber(), ep.getSqlState());
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
    public boolean ping() throws QueryException {
        lock.lock();
        try {
            final SendPingPacket pingPacket = new SendPingPacket();
            try {
                pingPacket.send(writer);
                ByteBuffer byteBuffer = packetFetcher.getReusableBuffer();
                return byteBuffer.get(0) == ReadResultPacketFactory.OK;
            } catch (IOException e) {
                throw new QueryException("Could not ping: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public AbstractQueryResult executeQuery(Query query) throws QueryException {
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
    public AbstractQueryResult executeQuery(final Query query, boolean streaming) throws QueryException {
        query.validate();
        this.moreResults = false;
        final SendTextQueryPacket packet = new SendTextQueryPacket(query);
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
    public AbstractQueryResult executeQuery(List<Query> queries, boolean streaming, boolean isRewritable, int rewriteOffset) throws QueryException {
        for (Query query : queries) {
            query.validate();
        }
        this.moreResults = false;
        AbstractQueryResult result = null;

        do {
            final SendTextQueryPacket packet = new SendTextQueryPacket(queries, isRewritable, rewriteOffset);
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


    private AbstractQueryResult executeQuery(Object queriesObj, SendTextQueryPacket packet, boolean streaming) throws QueryException {
        sendQuery(packet);
        return result(queriesObj, streaming);
    }

    private int sendQuery(SendTextQueryPacket packet)  throws QueryException {
    	if (!connected) {
    	    throw new QueryException("Could not send query: Connection is closed", -1, 
    	        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState());
    	}
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

    private AbstractQueryResult result(Object queriesObj, boolean streaming) throws QueryException {
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
    public AbstractQueryResult getResult(Object queriesObj, boolean streaming, boolean binaryProtocol) throws QueryException {
        RawPacket rawPacket = null;
        AbstractResultPacket resultPacket;
        try {
            rawPacket = packetFetcher.getReusableRawPacket();
            resultPacket = ReadResultPacketFactory.createResultPacket(rawPacket.getByteBuffer());

            if (resultPacket.getResultType() == AbstractResultPacket.ResultType.LOCALINFILE) {
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
                    String localInfile = localInfilePacket.getFileName();

                    try {
                        URL url = new URL(localInfile);
                        is = url.openStream();
                    } catch (IOException ioe) {
                        try {
                            is = new FileInputStream(localInfile);
                        } catch (FileNotFoundException f) {
                            writer.writeEmptyPacket(rawPacket.getPacketSeq() + 1);
                            ReadResultPacketFactory.createResultPacket(packetFetcher);
                            throw new QueryException("Could not send file : " + f.getMessage(), -1, "22000", f);
                        }
                    }
                } else {
                    is = localInfileInputStream;
                    localInfileInputStream = null;
                }

                writer.sendFile(is, rawPacket.getPacketSeq() + 1);
                is.close();
                resultPacket = ReadResultPacketFactory.createResultPacket(packetFetcher);
            }
        } catch (SocketTimeoutException ste) {
            this.close();
            throw new QueryException("Could not read resultset: " + ste.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), ste);
        } catch (IOException e) {
            try {
                if (writer != null && rawPacket != null) {
                    writer.writeEmptyPacket(rawPacket.getPacketSeq() + 1);
                    ReadResultPacketFactory.createResultPacket(packetFetcher);
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
                throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());

            case OK:
                final OkPacket okpacket = (OkPacket) resultPacket;
                serverStatus = okpacket.getServerStatus();
                this.moreResults = ((serverStatus & ServerStatus.MORE_RESULTS_EXISTS) != 0);
                this.hasWarnings = (okpacket.getWarnings() > 0);
                final AbstractQueryResult updateResult = new UpdateResult(okpacket.getAffectedRows(),
                        okpacket.getWarnings(),
                        okpacket.getMessage(),
                        okpacket.getInsertId());
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
                throw new QueryException("Could not parse result", (short) -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState());
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
    public AbstractQueryResult executeBatch(final List<Query> queries, boolean streaming, boolean isRewritable, int rewriteOffset)
            throws QueryException {
        for (Query query : queries) {
            query.validate();
        }

        this.moreResults = false;
        final SendTextQueryPacket packet = new SendTextQueryPacket(queries, isRewritable, rewriteOffset);
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
    public AbstractQueryResult executePreparedQueryAfterFailover(String sql, ParameterHolder[] parameters, PrepareResult oldPrepareResult,
                                                         MariaDbType[] parameterTypeHeader, boolean isStreaming) throws QueryException {
        PrepareResult prepareResult = prepare(sql);
        AbstractQueryResult queryResult = executePreparedQuery(sql, parameters, prepareResult, parameterTypeHeader, isStreaming);
        queryResult.setFailureObject(prepareResult);
        return queryResult;
    }

    @Override
    public AbstractQueryResult executePreparedQuery(String sql, ParameterHolder[] parameters, PrepareResult prepareResult,
                                                    MariaDbType[] parameterTypeHeader, boolean isStreaming) throws QueryException {
        this.moreResults = false;
        try {
            int parameterCount = parameters.length;
            //send binary data in a separate stream
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
            if (urlParser.getOptions().cachePrepStmts && prepareStatementCache.containsKey(sql)) {
                PrepareResult pr = prepareStatementCache.get(sql);
                pr.removeUse();
                if (!pr.hasToBeClose()) {
//                        log.debug("closing aborded, prepared statement used in another statement");
                    return;
                }
                prepareStatementCache.remove(sql);
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
        MasterProtocol copiedProtocol = new MasterProtocol(urlParser, new ReentrantLock());
        copiedProtocol.setHostAddress(getHostAddress());
        copiedProtocol.connect();
        //no lock, because there is already a query running that possessed the lock.
        copiedProtocol.executeQuery(new MariaDbQuery("KILL QUERY " + serverThreadId));
        copiedProtocol.close();
    }

    @Override
    public AbstractQueryResult getMoreResults(boolean streaming) throws QueryException {
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
     * Close active result.
     */
    public void closeIfActiveResult() {
        if (activeResult != null) {
            activeResult.close();
        }
    }

    public PrepareStatementCache prepareStatementCache() {
        return prepareStatementCache;
    }


}
