package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.ComStmtPrepare;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.ExecutionResult;
import org.mariadb.jdbc.internal.stream.MaxAllowedPacketException;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.BulkStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.io.IOException;
import java.util.List;

import static org.mariadb.jdbc.internal.util.ExceptionMapper.SqlStates.CONNECTION_EXCEPTION;
import static org.mariadb.jdbc.internal.util.ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION;

public abstract class AbstractBulkSend {
    private Protocol protocol;
    private PacketOutputStream writer;
    private ExecutionResult executionResult;
    private List<byte[]> queryParts;
    private List<ParameterHolder[]> parametersList;
    private int resultSetScrollType;
    private List<String> queries;
    private ServerPrepareResult serverPrepareResult;
    private boolean binaryProtocol;
    private boolean readPrepareStmtResult;
    private String sql;
    int statementId = -1;
    MariaDbType[] parameterTypeHeader;

    /**
     * Bulk execute for Server PreparedStatement.executeBatch (when no COM_MULTI)
     *
     * @param protocol protocol
     * @param writer outputStream
     * @param executionResult query results
     * @param serverPrepareResult Prepare result
     * @param parametersList parameters
     * @param resultSetScrollType resultSet scroll type
     * @param readPrepareStmtResult must execute prepare result
     * @param sql sql query.
     */
    public AbstractBulkSend(Protocol protocol, PacketOutputStream writer, ExecutionResult executionResult, ServerPrepareResult serverPrepareResult,
                            List<ParameterHolder[]> parametersList, int resultSetScrollType, boolean readPrepareStmtResult, String sql) {
        this.protocol = protocol;
        this.writer = writer;
        this.executionResult = executionResult;
        this.serverPrepareResult = serverPrepareResult;
        this.parametersList = parametersList;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = true;
        this.readPrepareStmtResult = readPrepareStmtResult;
        this.sql = sql;
    }

    /**
     * Bulk execute for client-sier PreparedStatement.executeBatch (no prepare).
     *
     * @param protocol current protocol
     * @param writer outputStream
     * @param executionResult results
     * @param queryParts query parts
     * @param parametersList parameters
     * @param resultSetScrollType resultSet scroll type
     */
    public AbstractBulkSend(Protocol protocol, PacketOutputStream writer, ExecutionResult executionResult, final List<byte[]> queryParts,
                            List<ParameterHolder[]> parametersList, int resultSetScrollType) {
        this.protocol = protocol;
        this.writer = writer;
        this.executionResult = executionResult;
        this.queryParts = queryParts;
        this.parametersList = parametersList;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = false;
        this.readPrepareStmtResult = false;
    }

    /**
     * Bulk execute for statement.executeBatch().
     *
     * @param protocol protocol
     * @param writer outputStream
     * @param executionResult results
     * @param queries query list
     * @param resultSetScrollType resultset type
     */
    public AbstractBulkSend(Protocol protocol, PacketOutputStream writer, ExecutionResult executionResult, List<String> queries,
                            int resultSetScrollType) {
        this.protocol = protocol;
        this.writer = writer;
        this.executionResult = executionResult;
        this.queries = queries;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = false;
        this.readPrepareStmtResult = false;
    }


    public abstract void sendCmd(PacketOutputStream writer, ExecutionResult executionResult, List<byte[]> queryParts,
                                 List<ParameterHolder[]> parametersList, List<String> queries, int paramCount, BulkStatus status,
                                 ServerPrepareResult serverPrepareResult) throws QueryException, IOException;

    public abstract QueryException handleResultException(QueryException qex, ExecutionResult executionResult, List<byte[]> queryParts,
                                 List<ParameterHolder[]> parametersList, List<String> queries, int currentCounter, BulkStatus status, int paramCount,
                                                         ServerPrepareResult serverPrepareResult)
            throws QueryException;

    public abstract int getParamCount();

    public abstract int getTotalExecutionNumber();


    /**
     * Execute Bulk execution (send packets by batch of  useBatchBulkSendNumber or when max packet is reached) before reading results.
     *
     * @param useComMulti can use com_multi
     * @return prepare result
     * @throws QueryException if connection
     */
    public ServerPrepareResult executeBatch(boolean useComMulti) throws QueryException {
        int paramCount = getParamCount();
        int totalExecutionNumber = getTotalExecutionNumber();
        QueryException exception = null;

        BulkStatus status = new BulkStatus();

        //Handle prepare if needed
        if (readPrepareStmtResult)   {
            parameterTypeHeader = new MariaDbType[paramCount];
            if (serverPrepareResult == null) {
                if (protocol.getOptions().cachePrepStmts) {
                    String key = new StringBuilder(protocol.getDatabase()).append("-").append(sql).toString();
                    serverPrepareResult = protocol.prepareStatementCache().get(key);
                    if (serverPrepareResult != null && !serverPrepareResult.incrementShareCounter()) {
                        //in cache but been de-allocated
                        serverPrepareResult = null;
                    }
                }
            }
            statementId = (serverPrepareResult == null) ? -1 : serverPrepareResult.getStatementId();
        } else if (serverPrepareResult != null) {
            statementId = serverPrepareResult.getStatementId();
        }

        try {
            do {
                if (useComMulti) {
                    writer.startPacket(0, true);
                    writer.buffer.put((byte) 0xfe);
                }

                status.sendSubCmdCounter = 0;

                //add prepare sub-command
                if (readPrepareStmtResult && statementId == -1) new ComStmtPrepare(protocol, sql).sendComMulti(writer);

                protocol.writeSavedSubCmd(status);

                while (status.sendCmdCounter < totalExecutionNumber && status.sendSubCmdCounter < protocol.getOptions().useBatchBulkSendNumber) {
                    if (!useComMulti) writer.startPacket(0, true);

                    status.subCmdInitialPosition = writer.buffer.position();
                    sendCmd(writer, executionResult, queryParts, parametersList, queries, paramCount, status, serverPrepareResult);
                    if (!writer.checkCurrentPacketAllowedSize()) {
                        status.lastSubCommand = new byte[writer.buffer.position() - status.subCmdInitialPosition];
                        //packet size > max_allowed_size -> need to send packet now without last command, and recreate new packet for additional data.
                        System.arraycopy(writer.buffer.array(), status.subCmdInitialPosition, status.lastSubCommand, 0,
                                writer.buffer.position() - status.subCmdInitialPosition);
                        writer.buffer.position(status.subCmdInitialPosition);
                        break;
                    }
                    status.sendSubCmdCounter++;
                    status.sendCmdCounter++;
                    if (!useComMulti) writer.finishPacketWithoutRelease();

                }

                if (useComMulti) writer.finishPacketWithoutRelease();

                if (readPrepareStmtResult && statementId == -1) {
                    try {
                        serverPrepareResult = new ComStmtPrepare(protocol, sql).read(protocol.getPacketFetcher());
                        statementId = serverPrepareResult.getStatementId();
                    } catch (QueryException queryException) {
                        exception = queryException;
                    }
                }

                //read all corresponding results
                for (int counter = 0; counter < status.sendSubCmdCounter; counter++) {
                    try {
                        protocol.getResult(executionResult, resultSetScrollType, binaryProtocol, true);
                    } catch (QueryException qex) {
                        if (exception == null) {
                            exception = handleResultException(qex, executionResult, queryParts, parametersList, queries, counter,
                                    status, paramCount, serverPrepareResult);
                        }
                    }
                }

                if (exception != null && ((readPrepareStmtResult && statementId == -1) || !protocol.getOptions().continueBatchOnError)) {
                    throw exception;
                }

            } while (status.sendCmdCounter < totalExecutionNumber);

            if (exception != null) throw exception;
            return serverPrepareResult;
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) protocol.connect();
            throw new QueryException("Could not send query: " + e.getMessage(), -1, INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, CONNECTION_EXCEPTION.getSqlState(), e);
        }

    }

    public ServerPrepareResult getServerPrepareResult() {
        return serverPrepareResult;
    }
}
