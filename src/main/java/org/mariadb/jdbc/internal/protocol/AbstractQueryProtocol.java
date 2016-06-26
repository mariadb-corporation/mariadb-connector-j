package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.packet.send.*;
import org.mariadb.jdbc.internal.queryresults.*;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.stream.MaxAllowedPacketException;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.packet.read.Packet;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
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

public class AbstractQueryProtocol extends AbstractConnectProtocol implements Protocol {

    private int transactionIsolationLevel = 0;
    private InputStream localInfileInputStream;
    private int maxRows;  /* max rows returned by a statement */

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL infos
     * @param lock      the lock for thread synchronisation
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
     * Prepare query on server side.
     * Will permit to know the parameter number of the query, and permit to send only the data on next results.
     *
     * For failover, two additional information are in the resultset object :
     * - current connection : Since server maintain a state of this prepare statement, all query will be executed on this particular connection.
     * - executeOnMaster : state of current connection when creating this prepareStatement (if was on master, will only be executed on master.
     * If was on a slave, can be execute temporary on master, but we keep this flag,
     * so when a slave is connected back to relaunch this query on slave)
     *
     * @param sql the query
     * @param executeOnMaster state of current connection when creating this prepareStatement
     * @return a ServerPrepareResult object that contain prepare result information.
     * @throws QueryException if any error occur on connection.
     */
    @Override
    public ServerPrepareResult prepare(String sql, boolean executeOnMaster) throws QueryException {
        lock.lock();
        try {

            if (activeStreamingResult != null) {
                throw new QueryException("There is an open result set on the current connection, which must be "
                        + "closed prior to executing a query");
            }

            checkClose();
            if (options.cachePrepStmts) {
                String key = new StringBuilder(database).append("-").append(sql).toString();
                ServerPrepareResult pr = serverPrepareStatementCache.get(key);
                if (pr != null && pr.incrementShareCounter()) {
                    return pr;
                }
            }

            writer.sendPreparePacket(sql);
            return readPrepareResult(sql);
        } catch (IOException e) {
            throw new QueryException(e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            lock.unlock();
        }
    }

    private ServerPrepareResult readPrepareResult(String sql)
            throws QueryException, IOException {

        Buffer buffer = packetFetcher.getReusableBuffer();
        byte firstByte = buffer.getByteAt(0);

        if (firstByte == Packet.ERROR) {
            ErrorPacket ep = new ErrorPacket(buffer);
            String message = ep.getMessage();
            throw new QueryException("Error preparing query: " + message + "\nIf a parameter type cannot be identified (example 'select ? `field1` from dual'). Use CAST function to solve this problem (example 'select CAST(? as integer) `field1` from dual')", ep.getErrorNumber(), ep.getSqlState());
        }

        if (firstByte == Packet.OK) {
                /* Prepared Statement OK */
            buffer.readByte(); /* skip field count */
            final int statementId = buffer.readInt();
            final int numColumns = buffer.readShort() & 0xffff;
            final int numParams = buffer.readShort() & 0xffff;
            buffer.readByte(); // reserved
            this.hasWarnings = buffer.readShort() > 0;
            ColumnInformation[] params = new ColumnInformation[numParams];
            if (numParams > 0) {
                for (int i = 0; i < numParams; i++) {
                    params[i] = new ColumnInformation(packetFetcher.getPacket());
                }
                readEofPacket();
            }
            ColumnInformation[] columns = new ColumnInformation[numColumns];
            if (numColumns > 0) {
                for (int i = 0; i < numColumns; i++) {
                    columns[i] = new ColumnInformation(packetFetcher.getPacket());
                }
                readEofPacket();
            }
            ServerPrepareResult serverPrepareResult = new ServerPrepareResult(sql, statementId, columns, params, this);
            if (options.cachePrepStmts && sql != null && sql.length() < options.prepStmtCacheSqlLimit) {
                String key = new StringBuilder(database).append("-").append(sql).toString();
                ServerPrepareResult cachedServerPrepareResult = serverPrepareStatementCache.put(key, serverPrepareResult);
                return cachedServerPrepareResult != null ? cachedServerPrepareResult : serverPrepareResult;
            }
            return serverPrepareResult;
        } else {
            throw new QueryException("Unexpected packet returned by server, first byte " + firstByte);
        }
    }

    private ServerPrepareResult getPrepareResultFromCacheIfNeeded(ServerPrepareResult serverPrepareResult, String sql)
            throws UnsupportedEncodingException {
        if (serverPrepareResult == null) {
            if (options.cachePrepStmts) {
                String key = new StringBuilder(database).append("-").append(sql).toString();
                serverPrepareResult = serverPrepareStatementCache.get(key);
                if (serverPrepareResult != null && !serverPrepareResult.incrementShareCounter()) {
                    //in cache but been de-allocated
                    return null;
                }
            }
        }
        return serverPrepareResult;
    }

    private void addPrepareSubCommand(String sql) throws UnsupportedEncodingException {
        byte[] sqlBytes = sql.getBytes("UTF-8");
        int prepareLengthCommand = sqlBytes.length + 1;

        //prepare length
        writer.buffer.put((byte) (prepareLengthCommand & 0xff));
        writer.buffer.put((byte) (prepareLengthCommand >>> 8));
        writer.buffer.put((byte) (prepareLengthCommand >>> 16));

        //prepare subCommand
        writer.buffer.put((byte) 0x16);
        writer.write(sqlBytes);
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
                executeQuery("ROLLBACK");
            }
        } catch (Exception e) {
            /* eat exception */
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setCatalog(final String database) throws QueryException {
        lock.lock();
        try {
            checkClose();
            final SendChangeDbPacket packet = new SendChangeDbPacket(database);
            packet.send(writer);
            final Buffer buffer = packetFetcher.getReusableBuffer();
            if (buffer.getByteAt(0) == Packet.ERROR) {
                final ErrorPacket ep = new ErrorPacket(buffer);
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
            checkClose();
            final SendPingPacket pingPacket = new SendPingPacket();
            try {
                pingPacket.send(writer);
                Buffer buffer = packetFetcher.getReusableBuffer();
                return buffer.getByteAt(0) == Packet.OK;
            } catch (IOException e) {
                throw new QueryException("Could not ping: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } finally {
            lock.unlock();
        }
    }

    private void sendLocalFile(ExecutionResult executionResult, String fileName) throws IOException, QueryException {
        // Server request the local file (LOCAL DATA LOCAL INFILE)
        // We do accept general URLs, too. If the localInfileStream is
        // set, use that.
        int seq = 2;
        InputStream is;
        if (localInfileInputStream == null) {
            if (!getUrlParser().getOptions().allowLocalInfile) {
                writer.writeEmptyPacket(seq++);
                throw new QueryException(
                        "Usage of LOCAL INFILE is disabled. To use it enable it via the connection property allowLocalInfile=true",
                        -1,
                        ExceptionMapper.SqlStates.FEATURE_NOT_SUPPORTED.getSqlState());
            }

            try {
                URL url = new URL(fileName);
                is = url.openStream();
            } catch (IOException ioe) {
                try {
                    is = new FileInputStream(fileName);
                } catch (FileNotFoundException f) {
                    writer.writeEmptyPacket(seq++);
                    packetFetcher.getReusableBuffer();
                    throw new QueryException("Could not send file : " + f.getMessage(), -1, "22000", f);
                }
            }
        } else {
            is = localInfileInputStream;
            localInfileInputStream = null;
        }
        writer.sendFile(is, seq);
        is.close();
        getResult(executionResult, ResultSet.TYPE_FORWARD_ONLY, false, true);
    }

    /**
     * Prepare query if needed, and execute send all executes in one packet (or more if &gt; max_allowed_packet).
     *
     * @param mustExecuteOnMaster must normally be executed on master connection
     * @param executionResult results
     * @param sql query
     * @param parameters parameters
     * @param resultSetScrollType  result scroll type
     * @return server prepare result
     * @throws QueryException if server return error, of connection fail
     */
    @Override
    public ServerPrepareResult prepareAndExecuteComMulti(boolean mustExecuteOnMaster, ExecutionResult executionResult,
                                                         String sql, ParameterHolder[] parameters,
                                                         int resultSetScrollType) throws QueryException {
        try {
            if (activeStreamingResult != null) {
                throw new QueryException("There is an open result set on the current connection, which must be "
                        + "closed prior to executing a query");
            }
            checkClose();
            this.moreResults = false;
            int parameterNb = parameters.length;
            ServerPrepareResult serverPrepareResult = getPrepareResultFromCacheIfNeeded(null, sql);
            int statementId = (serverPrepareResult == null) ? -1 : serverPrepareResult.getStatementId();

            //com multi init packet
            writer.startPacket(0, true);
            writer.buffer.put((byte) 0xfe);

            //add prepare sub-command
            if (statementId == -1) addPrepareSubCommand(sql);

            int subCmdInitialPosition;
            int subCmdEndPosition;

            //send long data
            for (int i = 0; i < parameterNb; i++) {
                if (parameters[i].isLongData()) {
                    //reserve 3 bytes for sub command length
                    subCmdInitialPosition = writer.buffer.position();
                    writer.assureBufferCapacity(3);
                    writer.buffer.position(subCmdInitialPosition + 3);

                    //add execute sub command
                    writer.write((byte) 0x18);
                    writer.writeInt(statementId);
                    writer.writeShort((short) i);
                    parameters[i].writeBinary(writer);

                    //write subCommand length
                    subCmdEndPosition = writer.buffer.position();
                    writer.buffer.position(subCmdInitialPosition);
                    writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) & 0xff));
                    writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) >>> 8));
                    writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) >>> 16));
                    writer.buffer.position(subCmdEndPosition);
                }
            }

            //reserve 3 bytes for sub command length
            subCmdInitialPosition = writer.buffer.position();
            writer.assureBufferCapacity(3);
            writer.buffer.position(subCmdInitialPosition + 3);

            //add execute sub command
            SendExecutePrepareStatementPacket.comStmtExecuteSubCommand(statementId, parameters, parameterNb, new MariaDbType[parameterNb], writer);

            //write subCommand length
            subCmdEndPosition = writer.buffer.position();
            writer.buffer.position(subCmdInitialPosition);
            writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) & 0xff));
            writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) >>> 8));
            writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) >>> 16));
            writer.buffer.position(subCmdEndPosition);

            writer.finishPacket();
            try {
                if (statementId == -1) serverPrepareResult = readPrepareResult(sql);
            } catch (QueryException queryException) {
                //if prepare fail, results must be read, before throwing the exception
                try {
                    getResult(executionResult, resultSetScrollType, true, true);
                } catch (QueryException qe) {}
                throw queryException;
            }
            getResult(executionResult, resultSetScrollType, true, true);
            return serverPrepareResult;
        } catch (IOException e) {
            throw new QueryException(e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * Execute Prepare if needed, and execute COM_STMT_EXECUTE queries in batch using COM_MULTI.
     *
     * @param mustExecuteOnMaster must normally be executed on master connection
     * @param serverPrepareResult prepare result. can be null if not prepared.
     * @param executionResult execution results
     * @param sql sql query if needed to be prepared
     * @param parameterList parameter list
     * @param resultSetScrollType result scroll type
     * @return Prepare result
     * @throws QueryException if parameter error or connection error occur.
     */
    public ServerPrepareResult prepareAndExecutesComMulti(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                                                          ExecutionResult executionResult, String sql,
                                                          List<ParameterHolder[]> parameterList, int resultSetScrollType)
            throws QueryException {
        try {
            //send prepare packet
            if (activeStreamingResult != null) {
                throw new QueryException("There is an open result set on the current connection, which must be "
                        + "closed prior to executing a query");
            }
            this.moreResults = false;
            int subCmdInitialPosition;
            int subCmdEndPosition;
            int subCmdCounter;
            int currentExecutionNumber = 0;
            int parameterNbPerExecute = parameterList.get(0).length;
            int totalExecutionNumber = parameterList.size();
            ParameterHolder[] parameters;
            byte[] lastSubCommand = null;
            QueryException exception = null;
            MariaDbType[] parameterTypeHeader = new MariaDbType[parameterNbPerExecute];

            //check prepare result
            serverPrepareResult = getPrepareResultFromCacheIfNeeded(serverPrepareResult, sql);
            int statementId = (serverPrepareResult == null) ? -1 : serverPrepareResult.getStatementId();

            do {
                subCmdCounter = 0;

                //using COM_MULTI
                writer.startPacket(0, true);
                writer.buffer.put((byte) 0xfe);

                if (statementId == -1) addPrepareSubCommand(sql);

                //in case of packet splitting, last subCommand that make packet > to max_allowed_packet
                if (lastSubCommand != null) {
                    writer.write(lastSubCommand, 0, lastSubCommand.length);
                    if (!writer.checkCurrentPacketAllowedSize()) {
                        //one sub-command is bigger than max packet Size
                        releasePrepareStatement(serverPrepareResult);
                        throw new QueryException("max_allowed_packet=" + getServerData("max_allowed_packet") + ". stream size "
                                + lastSubCommand.length + " is > to max_allowed_packet");
                    }
                    lastSubCommand = null;
                    subCmdCounter++;
                }

                for (; currentExecutionNumber < totalExecutionNumber; currentExecutionNumber++) {
                    parameters = parameterList.get(currentExecutionNumber);

                    //reserve 3 bytes for sub command length
                    subCmdInitialPosition = writer.buffer.position();
                    writer.assureBufferCapacity(3);
                    writer.buffer.position(subCmdInitialPosition + 3);

                    //add execute sub command
                    SendExecutePrepareStatementPacket.comStmtExecuteSubCommand(statementId, parameters,
                            parameterNbPerExecute, parameterTypeHeader, writer);

                    //write command size
                    subCmdEndPosition = writer.buffer.position();
                    writer.buffer.position(subCmdInitialPosition);
                    writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) & 0xff));
                    writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) >>> 8));
                    writer.buffer.put((byte) ((subCmdEndPosition - (subCmdInitialPosition + 3)) >>> 16));
                    writer.buffer.position(subCmdEndPosition);

                    //check that packet < max_allowed_packet, loop for next packet if so.
                    //otherwise, save sub-command, and set position to before this sub-command and send packet.
                    if (!writer.checkCurrentPacketAllowedSize()) {
                        lastSubCommand = new byte[subCmdEndPosition - subCmdInitialPosition];
                        //packet size > max_allowed_size -> need to send packet now without last command, and recreate new packet for additional data.
                        System.arraycopy(writer.buffer.array(), subCmdInitialPosition, lastSubCommand, 0, subCmdEndPosition - subCmdInitialPosition);
                        writer.buffer.position(subCmdInitialPosition);
                        break;
                    }
                    subCmdCounter++;
                }

                writer.finishPacket();

                //read prepare result
                if (statementId == -1) {
                    try {
                        serverPrepareResult = readPrepareResult(sql);
                        statementId = serverPrepareResult.getStatementId();
                    } catch (QueryException queryException) {
                        exception = queryException;
                    }
                }

                //read all execution result
                for (int counter = 0; counter < subCmdCounter; counter++) {
                    try {
                        getResult(executionResult, resultSetScrollType, true, true);
                    } catch (QueryException queryException) {
                        if (exception == null) exception = queryException;
                    }
                }
                if (exception != null) throw exception;

            } while (currentExecutionNumber < totalExecutionNumber || lastSubCommand != null);

            return serverPrepareResult;

        } catch (IOException e) {
            throw new QueryException(e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        }

    }

    @Override
    public void executePreparedQuery(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult, ExecutionResult executionResult,
                                     ParameterHolder[] parameters, int resultSetScrollType)
            throws QueryException {

        checkClose();
        this.moreResults = false;
        try {
            int parameterCount = serverPrepareResult.getParameters().length;
            //send binary data in a separate stream
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].isLongData()) {

                    writer.startPacket(0);
                    writer.buffer.put((byte) 0x18);
                    writer.buffer.putInt(serverPrepareResult.getStatementId());
                    writer.buffer.putShort((short) i);
                    parameters[i].writeBinary(writer);
                    writer.finishPacket();
                }
            }
            //send execute query
            new SendExecutePrepareStatementPacket(serverPrepareResult.getStatementId(), parameters,
                    parameterCount, serverPrepareResult.getParameterTypeHeader())
                    .send(writer);
            getResult(executionResult, resultSetScrollType, true, true);

        } catch (QueryException qex) {
            if (getOptions().dumpQueriesOnException || qex.getErrorCode() == 1064) {
                if (serverPrepareResult.getSql().length() > 1024) {
                    qex.setMessage(qex.getMessage() + "\nQuery is: " + serverPrepareResult.getSql().substring(0, 1024) + "...");
                } else {
                    qex.setMessage(qex.getMessage() + "\nQuery is: " + serverPrepareResult.getSql());
                }
            }
            if (qex.getCause() instanceof SocketTimeoutException) {
                throw new QueryException("Connection timed out", -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), qex);
            } else {
                throw qex;
            }
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
    }

    /**
     * Deallocate prepare statement if not used anymore.
     * @param serverPrepareResult allocation result
     * @throws QueryException if deallocation failed.
     */
    @Override
    public void releasePrepareStatement(ServerPrepareResult serverPrepareResult) throws QueryException {
        //If prepared cache is enable, the ServerPrepareResult can be shared in many PrepStatement,
        //so synchronised use count indicator will be decrement.
        serverPrepareResult.decrementShareCounter();

        //deallocate from server if not cached
        if (serverPrepareResult.canBeDeallocate()) {
            forceReleasePrepareStatement(serverPrepareResult.getStatementId());
        }
    }

    /**
     * Force release of prepare statement that are not used.
     * This method will be call when adding a new preparestatement in cache, so the packet can be send to server without
     * problem.
     *
     * @param statementId prepared statement Id to remove.
     * @throws QueryException if connection exception.
     */
    public void forceReleasePrepareStatement(int statementId) throws QueryException {
        lock.lock();
        try {
            checkClose();
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
     * @throws IOException    if Host is not responding
     */
    @Override
    public void cancelCurrentQuery() throws QueryException, IOException {
        MasterProtocol copiedProtocol = new MasterProtocol(urlParser, new ReentrantLock());
        copiedProtocol.setHostAddress(getHostAddress());
        copiedProtocol.connect();
        //no lock, because there is already a query running that possessed the lock.
        copiedProtocol.executeQuery("KILL QUERY " + serverThreadId);
        copiedProtocol.close();
    }

    @Override
    public void getMoreResults(ExecutionResult executionResult) throws QueryException {
        if (!hasMoreResults()) {
            return;
        }
        getResult(executionResult, ResultSet.TYPE_FORWARD_ONLY,
                (activeStreamingResult != null) ? activeStreamingResult.isBinaryEncoded() : moreResultsTypeBinary, false);
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
                executeQuery("set @@SQL_SELECT_LIMIT=DEFAULT");
            } else {
                executeQuery("set @@SQL_SELECT_LIMIT=" + max);
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
            executeQuery(query);
            transactionIsolationLevel = level;
        } finally {
            lock.unlock();
        }
    }

    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    private void checkClose() throws QueryException {
        if (!this.connected) {
            throw new QueryException("Connection is close", 1220, "08000");
        }
    }

    /**
     * Close active result.
     * @throws SQLException if socket error.
     */
    public void fetchActiveStreamingResult() throws SQLException {
        if (activeStreamingResult != null) {
            activeStreamingResult.fetchAllStreaming();
        }
    }

    public void executeQuery(final String sql) throws QueryException {
        executeQuery(isMasterConnection(), new SingleExecutionResult(null, 0, false, false), sql, ResultSet.TYPE_FORWARD_ONLY);
    }

    /**
     * Execute query.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param executionResult result
     * @param sql the query to executeInternal
     * @param resultSetScrollType resultSetScrollType
     * @throws QueryException exception
     */
    @Override
    public void executeQuery(boolean mustExecuteOnMaster, ExecutionResult executionResult,
                             final String sql, int resultSetScrollType) throws QueryException {
        checkClose();
        try {
            writer.sendTextPacket(sql.getBytes("UTF-8"));
            getResult(executionResult, resultSetScrollType, false, true);
        } catch (QueryException queryException) {
            if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
                String sqlQuery = sql;
                if (sqlQuery.length() > 1024) {
                    sqlQuery = sqlQuery.substring(0, 1024);
                }
                queryException.setMessage(queryException.getMessage() + "\nQuery is : " + sqlQuery);
            }
            throw queryException;
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

    }

    /**
     * Specific execution for batch allowMultipleQueries that has specific query for memory.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param executionResult result
     * @param queryParts query part
     * @param parameters parameters
     * @param resultSetScrollType resultsetScroll type
     * @throws QueryException exception
     */
    public void executeQuery(boolean mustExecuteOnMaster, ExecutionResult executionResult,
                             final List<byte[]> queryParts, ParameterHolder[] parameters,
                             int resultSetScrollType) throws QueryException {
        checkClose();
        int paramCount = queryParts.size() - 1;

        try {
            this.moreResults = false;

            if (paramCount == 0) {
                writer.sendTextPacket(queryParts.get(0));
            } else {
                writer.startPacket(0);
                writer.buffer.put((byte) 0x03);
                writer.write(queryParts.get(0));
                for (int i = 0; i < paramCount; i++) {
                    parameters[i].writeTo(writer);
                    writer.write(queryParts.get(i + 1));
                }
                writer.finishPacket();
            }
            getResult(executionResult, resultSetScrollType, false, true);

        } catch (QueryException queryException) {
            throwErrorWithQuery(queryParts, parameters, queryException, paramCount, false);
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * Specific execution for batch rewrite that has specific query for memory.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param executionResult result
     * @param queryParts query part
     * @param parameters parameters
     * @param resultSetScrollType resultsetScroll type
     * @param isRewritable is rewritable flag
     * @throws QueryException exception
     */
    public void executeQuery(boolean mustExecuteOnMaster, ExecutionResult executionResult,
                             final List<byte[]> queryParts, ParameterHolder[] parameters,
                             int resultSetScrollType, boolean isRewritable) throws QueryException {
        checkClose();
        int paramCount = queryParts.size() - 3;

        try {
            this.moreResults = false;
            writer.startPacket(0);
            writer.buffer.put((byte) 0x03);
            writer.write(queryParts.get(0));
            writer.write(queryParts.get(1));
            for (int i = 0; i < paramCount; i++) {
                parameters[i].writeTo(writer);
                writer.write(queryParts.get(i + 2));
            }
            writer.write(queryParts.get(paramCount + 2));

            writer.finishPacket();
            getResult(executionResult, resultSetScrollType, false, true);

        } catch (QueryException queryException) {
            throwErrorWithQuery(queryParts, parameters, queryException, paramCount, true);
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    private void throwErrorWithQuery(List<byte[]> queryParts, ParameterHolder[] parameters, QueryException queryException, int paramCount,
                                     boolean rewrite)
            throws QueryException {
        if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
            StringBuilder queryString = new StringBuilder(new String(queryParts.get(0)));
            if (rewrite) queryString.append(new String(queryParts.get(1)));
            for (int i = 0; i < paramCount; i++) {
                if (parameters != null && parameters.length > i) {
                    queryString.append(parameters[i]).append(new String(queryParts.get(i + (rewrite ? 2 : 1))));
                } else {
                    queryString.append("?").append(new String(queryParts.get(i + (rewrite ? 2 : 1))));
                }
            }
            if (rewrite) queryString.append(new String(queryParts.get(paramCount + 2)));
            addQueryInfo(queryString.toString(), queryException);
        }
        throw queryException;
    }


    /**
     * Execute list of queries not rewritable.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param queries list of queryes
     * @param resultSetScrollType resultSetScrollType
     * @throws QueryException exception
     */
    public void executeStmtBatch(boolean mustExecuteOnMaster, ExecutionResult executionResult, List<String> queries, int resultSetScrollType)
            throws QueryException {
        checkClose();
        int counter = 0;
        int size = queries.size();
        String sql = null;
        QueryException exception = null;
        for (; counter < size; counter++) {
            try {
                sql = queries.get(counter);
                writer.sendTextPacket(sql.getBytes("UTF-8"));
                getResult(executionResult, resultSetScrollType, false, true);
            } catch (QueryException queryException) {
                if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
                    addQueryInfo(sql, queryException);
                }
                if (getOptions().continueBatchOnError) {
                    if (exception == null) {
                        exception = queryException;
                    }
                } else {
                    throw queryException;
                }
            } catch (IOException e) {
                for (int i = 0; i < counter; i++) {
                    queries.remove(0);
                }
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Specific execution for batch allowMultipleQueries that has specific query for memory.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param executionResult result
     * @param queryParts query part
     * @param parameterList parameters
     * @param resultSetScrollType resultsetScroll type
     * @throws QueryException exception
     */
    public void executeBatchMultiple(boolean mustExecuteOnMaster, ExecutionResult executionResult,
                                     final List<byte[]> queryParts, List<ParameterHolder[]> parameterList,
                                     int resultSetScrollType) throws QueryException {
        checkClose();
        ParameterHolder[] parameters = null;
        int paramCount = queryParts.size() - 1;
        int currentIndex = 0;
        int totalParameterList = parameterList.size();

        try {
            this.moreResults = false;

            do {
                parameters = parameterList.get(currentIndex++);
                byte[] firstPart = queryParts.get(0);

                //calculate static length for packet splitting
                int staticLength = 1;
                for (int i = 0; i < queryParts.size(); i++) staticLength += queryParts.get(i).length;

                //write first query
                writer.startPacket(0);
                writer.write(0x03);
                writer.write(firstPart);
                for (int i = 0; i < paramCount; i++) {
                    parameters[i].writeTo(writer);
                    writer.write(queryParts.get(i + 1));
                }

                // write other, separate by ";"
                while (currentIndex < totalParameterList) {
                    parameters = parameterList.get(currentIndex);

                    //check packet length so to separate in multiple packet
                    int parameterLength = 0;
                    boolean knownParameterSize = true;
                    for (ParameterHolder parameter : parameters) {
                        long paramSize = parameter.getApproximateTextProtocolLength();
                        if (paramSize == -1) {
                            knownParameterSize = false;
                            break;
                        }
                        parameterLength += paramSize;
                    }

                    if (knownParameterSize) {
                        //We know the additional query part size. This permit :
                        // - to resize buffer size if needed (to avoid resize test every write)
                        // - if this query will be separated in a new packet.
                        if (writer.checkRewritableLength(staticLength + parameterLength)) {
                            writer.assureBufferCapacity(staticLength + parameterLength);
                            writer.buffer.put((byte) ';');
                            writer.buffer.put(firstPart, 0, firstPart.length);
                            for (int i = 0; i < paramCount; i++) {
                                parameters[i].writeUnsafeTo(writer);
                                writer.buffer.put(queryParts.get(i + 1));
                            }
                            currentIndex++;
                        } else {
                            break;
                        }
                    } else {
                        //we cannot know the additional query part size.
                        writer.write(';');
                        writer.write(firstPart, 0, firstPart.length);
                        for (int i = 0; i < paramCount; i++) {
                            parameters[i].writeTo(writer);
                            writer.write(queryParts.get(i + 1));
                        }
                        currentIndex++;
                    }
                }

                writer.finishPacket();
                getResult(executionResult, resultSetScrollType, false, true);
            } while (currentIndex < totalParameterList);

        } catch (QueryException queryException) {
            throwErrorWithQuery(queryParts, parameters, queryException, paramCount, false);
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * Specific execution for batch rewrite that has specific query for memory.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param executionResult result
     * @param queryParts query part
     * @param parameterList parameters
     * @param resultSetScrollType resultsetScroll type
     * @param isRewritable is rewritable flag
     * @throws QueryException exception
     */
    public void executeBatchRewrite(boolean mustExecuteOnMaster, ExecutionResult executionResult,
                                    final List<byte[]> queryParts, List<ParameterHolder[]> parameterList,
                                    int resultSetScrollType, boolean isRewritable) throws QueryException {
        checkClose();
        ParameterHolder[] parameters = null;
        int paramCount = queryParts.size() - 3;
        int currentIndex = 0;
        int totalParameterList = parameterList.size();

        try {
            this.moreResults = false;

            do {
                parameters = parameterList.get(currentIndex++);
                writer.startPacket(0);
                writer.buffer.put((byte)0x03);

                byte[] firstPart = queryParts.get(0);
                byte[] secondPart = queryParts.get(1);

                if (!isRewritable) {
                    //write first
                    writer.write(firstPart, 0, firstPart.length);
                    writer.write(secondPart, 0, secondPart.length);

                    int staticLength = 1;
                    for (int i = 0; i < queryParts.size(); i++) staticLength += queryParts.get(i).length;

                    for (int i = 0; i < paramCount; i++) {
                        parameters[i].writeTo(writer);
                        writer.write(queryParts.get(i + 2));
                    }
                    writer.write(queryParts.get(paramCount + 2));

                    // write other, separate by ";"
                    while (currentIndex < totalParameterList) {
                        parameters = parameterList.get(currentIndex);

                        //check packet length so to separate in multiple packet
                        int parameterLength = 0;
                        boolean knownParameterSize = true;
                        for (ParameterHolder parameter : parameters) {
                            long paramSize = parameter.getApproximateTextProtocolLength();
                            if (paramSize == -1) {
                                knownParameterSize = false;
                                break;
                            }
                            parameterLength += paramSize;
                        }

                        if (knownParameterSize) {
                            //We know the additional query part size. This permit :
                            // - to resize buffer size if needed (to avoid resize test every write)
                            // - if this query will be separated in a new packet.
                            if (writer.checkRewritableLength(staticLength + parameterLength)) {
                                writer.assureBufferCapacity(staticLength + parameterLength);
                                writer.buffer.put((byte)';');
                                writer.buffer.put(firstPart, 0, firstPart.length);
                                writer.buffer.put(secondPart, 0, secondPart.length);
                                for (int i = 0; i < paramCount; i++) {
                                    parameters[i].writeUnsafeTo(writer);
                                    writer.writeUnsafe(queryParts.get(i + 2));
                                }
                                writer.writeUnsafe(queryParts.get(paramCount + 2));
                                currentIndex++;
                            } else {
                                break;
                            }
                        } else {
                            //we cannot know the additional query part size.
                            writer.write(';');
                            writer.write(firstPart, 0, firstPart.length);
                            writer.write(secondPart, 0, secondPart.length);
                            for (int i = 0; i < paramCount; i++) {
                                parameters[i].writeTo(writer);
                                writer.write(queryParts.get(i + 2));
                            }
                            writer.write(queryParts.get(paramCount + 2));
                            currentIndex++;
                        }
                    }

                } else {
                    writer.write(firstPart, 0, firstPart.length);
                    writer.write(secondPart, 0, secondPart.length);
                    int lastPartLength = queryParts.get(paramCount + 2).length;
                    int intermediatePartLength = queryParts.get(1).length;

                    for (int i = 0; i < paramCount; i++) {
                        parameters[i].writeTo(writer);
                        writer.write(queryParts.get(i + 2));
                        intermediatePartLength += queryParts.get(i + 2).length;
                    }

                    while (currentIndex < totalParameterList) {
                        parameters = parameterList.get(currentIndex);

                        //check packet length so to separate in multiple packet
                        int parameterLength = 0;
                        boolean knownParameterSize = true;
                        for (ParameterHolder parameter : parameters) {
                            long paramSize = parameter.getApproximateTextProtocolLength();
                            if (paramSize == -1) {
                                knownParameterSize = false;
                                break;
                            }
                            parameterLength += paramSize;
                        }

                        if (knownParameterSize) {
                            //We know the additional query part size. This permit :
                            // - to resize buffer size if needed (to avoid resize test every write)
                            // - if this query will be separated in a new packet.
                            if (writer.checkRewritableLength(1 + parameterLength + intermediatePartLength + lastPartLength)) {
                                writer.assureBufferCapacity(1 + parameterLength + intermediatePartLength + lastPartLength);
                                writer.buffer.put((byte) ',');
                                writer.buffer.put(secondPart, 0, secondPart.length);

                                for (int i = 0; i < paramCount; i++) {
                                    parameters[i].writeUnsafeTo(writer);
                                    byte[] addPart = queryParts.get(i + 2);
                                    writer.buffer.put(addPart, 0, addPart.length);
                                }
                                currentIndex++;
                            } else {
                                break;
                            }
                        } else {
                            writer.write((byte) ',');
                            writer.write(secondPart, 0, secondPart.length);

                            for (int i = 0; i < paramCount; i++) {
                                parameters[i].writeTo(writer);
                                writer.write(queryParts.get(i + 2));
                            }
                            currentIndex++;
                        }
                    }
                    writer.write(queryParts.get(paramCount + 2));
                }

                writer.finishPacket();
                getResult(executionResult, resultSetScrollType, false, true);
            } while (currentIndex < totalParameterList);

        } catch (QueryException queryException) {
            throwErrorWithQuery(queryParts, parameters, queryException, paramCount, true);
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * Execute list of queries.
     * This method is used when using text batch statement and using rewriting (allowMultiQueries || rewriteBatchedStatements).
     * queries will be send to server according to max_allowed_packet size.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param queries list of queryes
     * @param resultSetScrollType resultSetScrollType
     * @throws QueryException exception
     */
    public void executeStmtBatchMultiple(boolean mustExecuteOnMaster, ExecutionResult executionResult, List<String> queries, int resultSetScrollType)
            throws QueryException {
        this.moreResults = false;
        String firstSql = null;
        int currentIndex = 0;
        int totalQueries = queries.size();
        QueryException exception = null;
        do {
            try {
                String sql = queries.get(currentIndex++);
                firstSql = sql;
                if (totalQueries == 1) {
                    writer.sendTextPacket(sql.getBytes("UTF-8"));
                    getResult(executionResult, resultSetScrollType, false, true);
                } else {
                    writer.startPacket(0);
                    writer.write(0x03);

                    //add query with ";"
                    writer.write(sql.getBytes("UTF-8"));

                    while (currentIndex < totalQueries) {
                        byte[] sqlByte = queries.get(currentIndex++).getBytes("UTF-8");
                        if (!writer.checkRewritableLength(sqlByte.length + 1)) {
                            break;
                        }
                        writer.write(';');
                        writer.write(sqlByte);
                    }

                    writer.finishPacket();
                    getResult(executionResult, resultSetScrollType, false, true);
                }
            } catch (QueryException queryException) {
                if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
                    addQueryInfo(firstSql, queryException);
                }
                if (getOptions().continueBatchOnError) {
                    if (exception == null) {
                        exception = queryException;
                    }
                } else {
                    throw queryException;
                }
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
        } while (currentIndex < totalQueries);

        if (exception != null) {
            throw exception;
        }
    }

    private void addQueryInfo(String sql, QueryException queryException) {
        if (sql.length() > 1024) {
            sql = sql.substring(0, 1024);
        }
        queryException.setMessage(queryException.getMessage() + "\nQuery is : " + sql);
    }

    @Override
    public ExecutionResult getResult(ExecutionResult executionResult, int resultSetScrollType, boolean binaryProtocol, boolean loadAllResults)
            throws QueryException {
        Buffer buffer;
        try {
            buffer = packetFetcher.getReusableBuffer();
        } catch (IOException e) {
            try {
                if (writer != null) {
                    writer.writeEmptyPacket(packetFetcher.getLastPacketSeq() + 1);
                    packetFetcher.getReusableBuffer();
                }
            } catch (IOException ee) { }
            throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        switch (buffer.getByteAt(0)) {
            case Packet.OK:
                //OK packet
                buffer.skipByte(); //fieldCount
                final long affectedRows = buffer.getLengthEncodedBinary();
                final long insertId = buffer.getLengthEncodedBinary();
                serverStatus = buffer.readShort();
                this.hasWarnings = (buffer.readShort() > 0);
                this.moreResults = ((serverStatus & ServerStatus.MORE_RESULTS_EXISTS) != 0);

                if (!loadAllResults) {
                    return new SingleExecutionResult(executionResult.getStatement(), 0, true, false, affectedRows, insertId);
                }

                executionResult.addStats(affectedRows, insertId, moreResults);
                while (moreResults && loadAllResults && executionResult.getFetchSize() == 0) {
                    //load additional results
                    executionResult.getCachedExecutionResults().add(getResult(executionResult, ResultSet.TYPE_FORWARD_ONLY,
                            (activeStreamingResult != null) ? activeStreamingResult.isBinaryEncoded() : moreResultsTypeBinary, false));
                }
                break;
            case Packet.ERROR:
                //Error packet
                this.moreResults = false;
                this.hasWarnings = false;
                buffer.skipByte();
                int errorNumber = buffer.readShort();
                String message;
                String sqlState;
                if (buffer.readByte() == '#') {
                    sqlState = new String(buffer.readRawBytes(5));
                    message = buffer.readString(StandardCharsets.UTF_8);
                } else {
                    // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
                    message = new String(buffer.buf, buffer.position, buffer.limit, StandardCharsets.UTF_8);
                    sqlState = "HY000";
                }
                executionResult.addStats(Statement.EXECUTE_FAILED, Statement.SUCCESS_NO_INFO, moreResults);
                throw new QueryException(message, errorNumber, sqlState);

            case Packet.LOCAL_INFILE:
                //Send fileName
                buffer.getLengthEncodedBinary(); //field count
                String fileName = buffer.readString(StandardCharsets.UTF_8);
                try {
                    sendLocalFile(executionResult, fileName);
                } catch (IOException e) {
                    try {
                        if (writer != null) {
                            writer.writeEmptyPacket(packetFetcher.getLastPacketSeq() + 1);
                            packetFetcher.getReusableBuffer();
                        }
                    } catch (IOException ee) { }
                    throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                            ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
                break;
            case Packet.EOF:
                if (buffer.remaining() < 9) {
                    throw new QueryException("Could not parse result", (short) -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState());
                }

            default:
                this.hasWarnings = false;
                long fieldCount = buffer.getLengthEncodedBinary();

                try {
                    boolean callableResult = false;

                    //read columns infos
                    ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];
                    for (int i = 0; i < fieldCount; i++) {
                        ci[i] = new ColumnInformation(packetFetcher.getPacket());
                    }

                    //read EOF packet
                    Buffer bufferEof = packetFetcher.getReusableBuffer();
                    if (bufferEof.getByteAt(0) != Packet.EOF) {
                        throw new QueryException("Packets out of order when reading field packets, expected was EOF stream. "
                                + "Packet contents (hex) = " + MasterProtocol.hexdump(bufferEof.buf, 0));
                    } else if (executionResult.isCanHaveCallableResultset() || !isMariaServer) {
                        //Identify if this is a "callable OUT packet" (callableResult=true)
                        //needed because :
                        // - will permit for callableStatement to identify the output result packet
                        // - after "OUT packet", a OK packet is send, but mysql send the "OUT packet with a bad "more result flag",
                        //   so need to check that this is a "OUT packet" to known there is another packet.
                        EndOfFilePacket endOfFilePacket = new EndOfFilePacket(bufferEof);
                        callableResult = (endOfFilePacket.getStatusFlags() & ServerStatus.PS_OUT_PARAMETERS) != 0;
                    }

                    //fetch Select result
                    MariaSelectResultSet mariaSelectResultset = new MariaSelectResultSet(ci, executionResult.getStatement(), this, packetFetcher,
                            binaryProtocol, resultSetScrollType, executionResult.getFetchSize(), callableResult);
                    mariaSelectResultset.initFetch();

                    if (!executionResult.isSelectPossible()) throw new QueryException("Select command are not permitted via executeBatch() command");
                    if (!loadAllResults) return new SingleExecutionResult(executionResult.getStatement(), 0, true, false, mariaSelectResultset);

                    executionResult.addResultSet(mariaSelectResultset, moreResults);

                    //load additional results
                    while (moreResults && loadAllResults && executionResult.getFetchSize() == 0) {
                        executionResult.getCachedExecutionResults().add(getResult(executionResult, ResultSet.TYPE_FORWARD_ONLY,
                                (activeStreamingResult != null) ? activeStreamingResult.isBinaryEncoded() : moreResultsTypeBinary, false));
                    }

                } catch (IOException e) {
                    throw new QueryException("Could not read result set: " + e.getMessage(),
                            -1,
                            ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                            e);
                }
                break;

        }
        return executionResult;
    }

    public void prologProxy(ServerPrepareResult serverPrepareResult, ExecutionResult executionResult, int maxRows, boolean hasProxy,
                            MariaDbConnection connection, Statement statement) throws SQLException {
        prolog(executionResult, maxRows, hasProxy, connection, statement);
    }

    /**
     * Preparation before command.
     * @param executionResult result
     * @param maxRows query max rows
     * @param hasProxy has proxy
     * @param connection current connection
     * @param statement current statement
     * @throws SQLException if any error occur.
     */
    public void prolog(ExecutionResult executionResult, int maxRows, boolean hasProxy, MariaDbConnection connection, Statement statement)
            throws SQLException {
        if (explicitClosed) {
            throw new SQLException("execute() is called on closed connection");
        }
        //old failover handling
        if (!hasProxy) {
            if (shouldReconnectWithoutProxy()) {
                try {
                    connectWithoutProxy();
                } catch (QueryException qe) {
                    ExceptionMapper.throwException(qe, connection, statement);
                }
            }
        }

        try {
            setMaxRows(maxRows);
            fetchActiveStreamingResult();
            while (hasMoreResults()) {
                getMoreResults(executionResult);
            }
        } catch (QueryException qe) {
            ExceptionMapper.throwException(qe, connection, statement);
        }

        connection.reenableWarnings();
    }

}
