package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.packet.ComStmtPrepare;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.ExecutionResult;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.util.List;
import java.util.concurrent.Callable;

public class BulkRead implements Callable<PrepareResult> {

    private final ComStmtPrepare comStmtPrepare;
    private final int nbResult;
    private final int sendCmdCounter;
    private final boolean handleMinusOnePrepare;
    private final Protocol protocol;
    private final boolean readPrepareStmtResult;
    private PrepareResult prepareResult;
    private ExecutionResult executionResult;
    private final int resultSetScrollType;
    private boolean binaryProtocol;
    private final AbstractBulkSend bulkSend;
    private int paramCount;
    private final List<ParameterHolder[]> parametersList;
    private final List<String> queries;


    /**
     * Read results async to avoid local and remote networking stack buffer overflow "lock".
     *
     * @param comStmtPrepare current prepare
     * @param nbResult number of command send
     * @param sendCmdCounter initial command counter
     * @param handleMinusOnePrepare can use "-1" as last prepared query
     * @param protocol protocol
     * @param readPrepareStmtResult must read prepare statement result
     * @param bulkSend bulk sender object
     * @param paramCount number of parameters
     * @param resultSetScrollType resultset scroll type
     * @param binaryProtocol using binary protocol
     * @param executionResult execution result
     * @param parametersList parameter list
     * @param queries queries
     * @param prepareResult prepare result
     */
    public BulkRead(ComStmtPrepare comStmtPrepare, int nbResult, int sendCmdCounter, boolean handleMinusOnePrepare, Protocol protocol,
                    boolean readPrepareStmtResult, AbstractBulkSend bulkSend, int paramCount, int resultSetScrollType, boolean binaryProtocol,
                    ExecutionResult executionResult, List<ParameterHolder[]> parametersList, List<String> queries, PrepareResult prepareResult) {
        this.comStmtPrepare = comStmtPrepare;
        this.nbResult = nbResult;
        this.sendCmdCounter = sendCmdCounter;
        this.handleMinusOnePrepare = handleMinusOnePrepare;
        this.protocol = protocol;
        this.readPrepareStmtResult = readPrepareStmtResult;
        this.bulkSend = bulkSend;
        this.paramCount = paramCount;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = binaryProtocol;
        this.executionResult = executionResult;
        this.parametersList = parametersList;
        this.queries = queries;
        this.prepareResult = prepareResult;
    }

    @Override
    public PrepareResult call() throws Exception {
        QueryException exception = null;

        if (readPrepareStmtResult) {
            try {
                prepareResult = comStmtPrepare.read(protocol.getPacketFetcher());
            } catch (QueryException queryException) {
                exception = queryException;
            }
        }

        //read all corresponding results
        for (int counter = 0; counter < nbResult; counter++) {
            try {
                protocol.getResult(executionResult, resultSetScrollType, binaryProtocol, true);
            } catch (QueryException qex) {
                if (exception == null) {
                    exception = bulkSend.handleResultException(qex, executionResult, parametersList, queries, counter,
                            sendCmdCounter, paramCount, prepareResult);
                }
            }
        }

        if (exception != null) throw exception;

        return prepareResult;
    }

}
