package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.packet.ComStmtPrepare;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.ExecutionResult;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.util.List;
import java.util.concurrent.Callable;

public class AsyncMultiRead implements Callable<AsyncMultiReadResult> {

    private final ComStmtPrepare comStmtPrepare;
    private final int nbResult;
    private final int sendCmdCounter;
    private final Protocol protocol;
    private final boolean readPrepareStmtResult;
    private final int resultSetScrollType;
    private final AbstractMultiSend bulkSend;
    private final List<ParameterHolder[]> parametersList;
    private final List<String> queries;
    private ExecutionResult executionResult;
    private boolean binaryProtocol;
    private int paramCount;
    private AsyncMultiReadResult asyncMultiReadResult;


    /**
     * Read results async to avoid local and remote networking stack buffer overflow "lock".
     *
     * @param comStmtPrepare        current prepare
     * @param nbResult              number of command send
     * @param sendCmdCounter        initial command counter
     * @param protocol              protocol
     * @param readPrepareStmtResult must read prepare statement result
     * @param bulkSend              bulk sender object
     * @param paramCount            number of parameters
     * @param resultSetScrollType   resultset scroll type
     * @param binaryProtocol        using binary protocol
     * @param executionResult       execution result
     * @param parametersList        parameter list
     * @param queries               queries
     * @param prepareResult         prepare result
     */
    public AsyncMultiRead(ComStmtPrepare comStmtPrepare, int nbResult, int sendCmdCounter,
                          Protocol protocol, boolean readPrepareStmtResult, AbstractMultiSend bulkSend, int paramCount,
                          int resultSetScrollType, boolean binaryProtocol, ExecutionResult executionResult,
                          List<ParameterHolder[]> parametersList, List<String> queries, PrepareResult prepareResult) {
        this.comStmtPrepare = comStmtPrepare;
        this.nbResult = nbResult;
        this.sendCmdCounter = sendCmdCounter;
        this.protocol = protocol;
        this.readPrepareStmtResult = readPrepareStmtResult;
        this.bulkSend = bulkSend;
        this.paramCount = paramCount;
        this.resultSetScrollType = resultSetScrollType;
        this.binaryProtocol = binaryProtocol;
        this.executionResult = executionResult;
        this.parametersList = parametersList;
        this.queries = queries;
        this.asyncMultiReadResult = new AsyncMultiReadResult(prepareResult);
    }

    @Override
    public AsyncMultiReadResult call() throws Exception {
        // avoid synchronisation of calls for write and read
        // since technically, getResult can be called before the write is send.
        // Other solution would have been to synchronised write and read, but would have been less performant,
        // just to have this timeout according to set value
        if (protocol.getOptions().socketTimeout != null) protocol.changeSocketSoTimeout(0);

        if (readPrepareStmtResult) {
            try {
                asyncMultiReadResult.setPrepareResult(comStmtPrepare.read(protocol.getPacketFetcher()));
            } catch (QueryException queryException) {
                asyncMultiReadResult.setException(queryException);
            }
        }

        //read all corresponding results
        for (int counter = 0; counter < nbResult; counter++) {
            try {
                protocol.getResult(executionResult, resultSetScrollType, binaryProtocol, true);
            } catch (QueryException qex) {
                if (asyncMultiReadResult.getException() == null) {
                    asyncMultiReadResult.setException(bulkSend.handleResultException(qex, executionResult,
                            parametersList, queries, counter, sendCmdCounter, paramCount,
                            asyncMultiReadResult.getPrepareResult()));
                }
            }
        }

        if (protocol.getOptions().socketTimeout != null) {
            protocol.changeSocketSoTimeout(protocol.getOptions().socketTimeout);
        }

        return asyncMultiReadResult;
    }

}
