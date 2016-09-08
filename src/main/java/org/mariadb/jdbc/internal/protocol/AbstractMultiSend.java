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

package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.ComStmtPrepare;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.ExecutionResult;
import org.mariadb.jdbc.internal.stream.MaxAllowedPacketException;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.BulkStatus;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mariadb.jdbc.internal.util.ExceptionMapper.SqlStates.CONNECTION_EXCEPTION;
import static org.mariadb.jdbc.internal.util.ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION;

public abstract class AbstractMultiSend {

    private static final ThreadPoolExecutor readScheduler = SchedulerServiceProviderHolder.getBulkScheduler();

    private Protocol protocol;
    private PacketOutputStream writer;
    private ExecutionResult executionResult;
    private List<ParameterHolder[]> parametersList;
    private PrepareResult prepareResult;
    private int resultSetScrollType;
    private List<String> queries;
    private boolean binaryProtocol;
    private boolean readPrepareStmtResult;
    private String sql;
    int statementId = -1;
    MariaDbType[] parameterTypeHeader;

    /**
     * Bulk execute for Server PreparedStatement.executeBatch (when no COM_MULTI)
     *
     * @param protocol              protocol
     * @param writer                outputStream
     * @param executionResult       query results
     * @param serverPrepareResult   Prepare result
     * @param parametersList        parameters
     * @param resultSetScrollType   resultSet scroll type
     * @param readPrepareStmtResult must execute prepare result
     * @param sql                   sql query.
     */
    public AbstractMultiSend(Protocol protocol, PacketOutputStream writer, ExecutionResult executionResult, ServerPrepareResult serverPrepareResult,
                             List<ParameterHolder[]> parametersList, int resultSetScrollType, boolean readPrepareStmtResult, String sql) {
        this.protocol = protocol;
        this.writer = writer;
        this.executionResult = executionResult;
        this.prepareResult = serverPrepareResult;
        this.parametersList = parametersList;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = true;
        this.readPrepareStmtResult = readPrepareStmtResult;
        this.sql = sql;
    }

    /**
     * Bulk execute for client-sier PreparedStatement.executeBatch (no prepare).
     *
     * @param protocol            current protocol
     * @param writer              outputStream
     * @param executionResult     results
     * @param clientPrepareResult clientPrepareResult
     * @param parametersList      parameters
     * @param resultSetScrollType resultSet scroll type
     */
    public AbstractMultiSend(Protocol protocol, PacketOutputStream writer, ExecutionResult executionResult,
                             final ClientPrepareResult clientPrepareResult, List<ParameterHolder[]> parametersList, int resultSetScrollType) {
        this.protocol = protocol;
        this.writer = writer;
        this.executionResult = executionResult;
        this.prepareResult = clientPrepareResult;
        this.parametersList = parametersList;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = false;
        this.readPrepareStmtResult = false;
    }

    /**
     * Bulk execute for statement.executeBatch().
     *
     * @param protocol            protocol
     * @param writer              outputStream
     * @param executionResult     results
     * @param queries             query list
     * @param resultSetScrollType resultset type
     */
    public AbstractMultiSend(Protocol protocol, PacketOutputStream writer, ExecutionResult executionResult, List<String> queries,
                             int resultSetScrollType) {
        this.protocol = protocol;
        this.writer = writer;
        this.executionResult = executionResult;
        this.queries = queries;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = false;
        this.readPrepareStmtResult = false;
    }

    public abstract void sendCmd(PacketOutputStream writer, ExecutionResult executionResult,
                                 List<ParameterHolder[]> parametersList, List<String> queries, int paramCount, BulkStatus status,
                                 PrepareResult prepareResult) throws QueryException, IOException;

    public abstract boolean sendSubCmd(PacketOutputStream writer, ExecutionResult executionResult,
                                 List<ParameterHolder[]> parametersList, List<String> queries, int paramCount, BulkStatus status,
                                 PrepareResult prepareResult) throws QueryException, IOException;


    public abstract QueryException handleResultException(QueryException qex, ExecutionResult executionResult,
                                                         List<ParameterHolder[]> parametersList, List<String> queries, int currentCounter,
                                                         int sendCmdCounter, int paramCount, PrepareResult prepareResult)
            throws QueryException;

    public abstract int getParamCount();

    public abstract int getTotalExecutionNumber();


    public PrepareResult getPrepareResult() {
        return prepareResult;
    }

    /**
     * Execute Bulk execution (send packets by batch of  useBatchMultiSendNumber or when max packet is reached) before reading results.
     *
     * @param hasComMultiCapacity can use '-1' for last prepareStatementId
     * @return prepare result
     * @throws QueryException if any error occur
     */
    public PrepareResult executeBatch(boolean hasComMultiCapacity) throws QueryException {
        int paramCount = getParamCount();
        if (binaryProtocol) {
            if (readPrepareStmtResult) {
                parameterTypeHeader = new MariaDbType[paramCount];
                if (prepareResult == null) {
                    if (protocol.getOptions().cachePrepStmts) {
                        String key = new StringBuilder(protocol.getDatabase()).append("-").append(sql).toString();
                        prepareResult = protocol.prepareStatementCache().get(key);
                        if (prepareResult != null && !((ServerPrepareResult) prepareResult).incrementShareCounter()) {
                            //in cache but been de-allocated
                            prepareResult = null;
                        }
                    }
                }
                statementId = (prepareResult == null) ? -1 : ((ServerPrepareResult) prepareResult).getStatementId();
            } else if (prepareResult != null) {
                statementId = ((ServerPrepareResult) prepareResult).getStatementId();
            }
        }
        if (hasComMultiCapacity) return executeComMultiBatch(paramCount);
        return executeBatchStandard(paramCount);
    }

    /**
     * Execute Bulk execution (send packets by batch of  useBatchMultiSendNumber or when max packet is reached) before reading results.
     *
     * @param paramCount parameter counter
     * @return prepare result
     * @throws QueryException if any error occur
     */
    private PrepareResult executeBatchStandard(int paramCount) throws QueryException {
        int totalExecutionNumber = getTotalExecutionNumber();
        QueryException exception = null;
        BulkStatus status = new BulkStatus();

        ComStmtPrepare comStmtPrepare = null;
        FutureTask<AsyncMultiReadResult> futureReadTask = null;

        int requestNumberByBulk;
        try {
            do {
                status.sendSubCmdCounter = 0;
                requestNumberByBulk = Math.min(totalExecutionNumber - status.sendCmdCounter, protocol.getOptions().useBatchMultiSendNumber);
                protocol.changeSocketTcpNoDelay(false); //enable NAGLE algorithm temporary.


                //add prepare sub-command
                if (readPrepareStmtResult && prepareResult == null) {

                    comStmtPrepare = new ComStmtPrepare(protocol, sql);
                    comStmtPrepare.send(writer);

                    //read prepare result
                    try {
                        prepareResult = comStmtPrepare.read(protocol.getPacketFetcher());
                        statementId = ((ServerPrepareResult) prepareResult).getStatementId();
                        paramCount = getParamCount();
                    } catch (QueryException queryException) {
                        throw queryException;
                    }
                }

                for (; status.sendSubCmdCounter < requestNumberByBulk;) {
                    sendCmd(writer, executionResult, parametersList, queries, paramCount, status, prepareResult);
                    status.sendSubCmdCounter++;
                    status.sendCmdCounter++;
                    writer.finishPacketWithoutRelease();

                    if (futureReadTask == null) {
                        futureReadTask = new FutureTask<>(new AsyncMultiRead(comStmtPrepare, requestNumberByBulk, (status.sendCmdCounter - 1),
                                protocol, false, this, paramCount,
                                resultSetScrollType, binaryProtocol, executionResult, parametersList, queries, prepareResult));
                        readScheduler.execute(futureReadTask);
                    }
                }

                protocol.changeSocketTcpNoDelay(protocol.getOptions().tcpNoDelay);
                try {
                    AsyncMultiReadResult asyncMultiReadResult = futureReadTask.get();

                    if (binaryProtocol && prepareResult == null && asyncMultiReadResult.getPrepareResult() != null) {
                        prepareResult = asyncMultiReadResult.getPrepareResult();
                        statementId = ((ServerPrepareResult) prepareResult).getStatementId();
                        paramCount = prepareResult.getParamCount();
                    }

                    if (asyncMultiReadResult.getException() != null) {
                        if (((readPrepareStmtResult && prepareResult == null) || !protocol.getOptions().continueBatchOnError)) {
                            throw asyncMultiReadResult.getException();
                        } else {
                            exception = asyncMultiReadResult.getException();
                        }
                    }
                } catch (ExecutionException executionException) {
                    if (executionException.getCause() == null) {
                        throw new QueryException("Error reading results " + executionException.getMessage());
                    }
                    throw new QueryException("Error reading results " + executionException.getCause().getMessage());
                } catch (InterruptedException interruptedException) {
                } finally {
                    //bulk can prepare, and so if prepare cache is enable, can replace an already cached prepareStatement
                    //this permit to release those old prepared statement without conflict.
                    protocol.forceReleaseWaitingPrepareStatement();
                }

                futureReadTask = null;

            } while (status.sendCmdCounter < totalExecutionNumber);
            if (exception != null) throw exception;
            return prepareResult;
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) protocol.connect();
            throw new QueryException("Could not send query: " + e.getMessage(), -1, INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, CONNECTION_EXCEPTION.getSqlState(), e);
        } finally {
            writer.releaseBufferIfNotLogging();
        }

    }


    /**
     * Execute com_multi Bulk execution.
     *
     * @param paramCount parameter counter
     * @return prepare result
     * @throws QueryException if any error occur
     */
    public PrepareResult executeComMultiBatch(int paramCount) throws QueryException {
        int totalExecutionNumber = getTotalExecutionNumber();
        QueryException exception = null;
        BulkStatus status = new BulkStatus();

        ComStmtPrepare comStmtPrepare = null;

        try {
            do {
                status.sendSubCmdCounter = 0;
                status.currentLongDataParam = 0;

                writer.startPacket(0);
                writer.buffer.put((byte) 254);

                if (status.notSendCmd != null) {
                    writer.writeByteArrayLength(status.notSendCmd);
                    if (setRealCmdSize(status)) {
                        int cmdLength = status.notSendCmd.length;
                        int subCommandLength;
                        if (cmdLength < 251) {
                            subCommandLength = cmdLength + 1;
                        } else if (cmdLength < 65536) {
                            subCommandLength = cmdLength + 3;
                        } else if (cmdLength < 16777216) {
                            subCommandLength = cmdLength + 4;
                        } else {
                            subCommandLength = cmdLength + 9;
                        }
                        throw new QueryException("max_allowed_packet=" + (writer.getMaxAllowedPacket() - 1) + ". Query size "
                                + (subCommandLength + 1) + " is > to max_allowed_packet");
                    }
                    status.notSendCmd = null;
                }

                //send PREPARE sub-command
                if (readPrepareStmtResult && prepareResult == null) {
                    comStmtPrepare = new ComStmtPrepare(protocol, sql);
                    comStmtPrepare.sendSubCmd(writer);
                }
                int initialCounter = status.sendCmdCounter;
                do {

                    if (sendSubCmd(writer, executionResult, parametersList, queries, paramCount, status, prepareResult)) {
                        break;
                    }

                    status.currentLongDataParam = 0;
                    status.sendSubCmdCounter++;
                    status.sendCmdCounter++;

                } while (status.sendCmdCounter < totalExecutionNumber);

                //send command if not empty
                if (readPrepareStmtResult && prepareResult == null || status.sendSubCmdCounter > 0) {
                    writer.finishPacketWithoutRelease();
                }

                if (readPrepareStmtResult && prepareResult == null) {
                    prepareResult = comStmtPrepare.read(protocol.getPacketFetcher());
                }

                for (int counter = 0; counter < status.sendSubCmdCounter; counter++) {
                    try {
                        protocol.getResult(executionResult, resultSetScrollType, binaryProtocol, true);
                    } catch (QueryException qex) {
                        if (exception == null) {
                            exception = handleResultException(qex, executionResult,
                                    parametersList, queries, counter, initialCounter, paramCount,
                                    prepareResult);
                        }
                    }
                }

            } while (status.sendCmdCounter < totalExecutionNumber);

            if (exception != null) throw exception;
            return prepareResult;

        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) protocol.connect();
            throw new QueryException("Could not send query: " + e.getMessage(), -1, INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, CONNECTION_EXCEPTION.getSqlState(), e);
        } finally {
            writer.releaseBufferIfNotLogging();
        }
    }

    /**
     * Set real command size to reserved bytes, or set last command in buffer to send current packet if packet max size is exceeded.
     * @param status current status data
     * @return true if packet size exceeded
     * @throws QueryException if connection error occur.
     */
    public boolean setRealCmdSize(BulkStatus status) throws QueryException {
        if (writer.buffer.limit() - 4 > writer.getMaxAllowedPacket() - 1) {
            //buffer size > max_allowed_packet. Will send query without the last sub-command.
            //save last sub-command
            status.notSendCmd = new byte[writer.buffer.position() - (status.initialPosition + 9)];
            System.arraycopy(writer.buffer.array(), status.initialPosition + 9, status.notSendCmd, 0, status.notSendCmd.length);
            writer.buffer.position(status.initialPosition);
            return true;
        }

        //set real command size to reserved bytes
        int endPosition = writer.buffer.position();
        long commandLength = endPosition - (status.initialPosition + 9);
        writer.buffer.position(status.initialPosition + 1);
        writer.buffer.putLong(commandLength);
        writer.buffer.position(endPosition);
        return false;
    }

    /**
     * Reserve some byte that indicate command size.
     * (since all query size cannot be known in advance
     *
     * @param status current status informations
     */
    public void reserveCmdLength(BulkStatus status) {
        status.initialPosition = writer.buffer.position();
        writer.assureBufferCapacity(9);
        writer.buffer.put((byte) 0xfe);
        writer.buffer.putLong(0L);
    }

}
