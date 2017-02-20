package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.packet.ComStmtPrepare;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.Results;
import org.mariadb.jdbc.internal.util.BulkStatus;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import static org.mariadb.jdbc.internal.util.SqlStates.INTERRUPTED_EXCEPTION;

public class AsyncMultiRead implements Callable<AsyncMultiReadResult> {

    private final ComStmtPrepare comStmtPrepare;
    private final BulkStatus status;
    private final int sendCmdInitialCounter;
    private final Protocol protocol;
    private final boolean readPrepareStmtResult;
    private Results results;
    private final AbstractMultiSend bulkSend;
    private final List<ParameterHolder[]> parametersList;
    private final List<String> queries;
    private boolean binaryProtocol;
    private int paramCount;
    private AsyncMultiReadResult asyncMultiReadResult;


    /**
     * Read results async to avoid local and remote networking stack buffer overflow "lock".
     *
     * @param comStmtPrepare        current prepare
     * @param status                bulk status
     * @param protocol              protocol
     * @param readPrepareStmtResult must read prepare statement result
     * @param bulkSend              bulk sender object
     * @param paramCount            number of parameters
     * @param binaryProtocol        using binary protocol
     * @param results               execution result
     * @param parametersList        parameter list
     * @param queries               queries
     * @param prepareResult         prepare result
     */
    public AsyncMultiRead(ComStmtPrepare comStmtPrepare, BulkStatus status,
                          Protocol protocol, boolean readPrepareStmtResult, AbstractMultiSend bulkSend, int paramCount,
                          boolean binaryProtocol, Results results,
                          List<ParameterHolder[]> parametersList, List<String> queries, PrepareResult prepareResult) {
        this.comStmtPrepare = comStmtPrepare;
        this.status = status;
        this.sendCmdInitialCounter = status.sendCmdCounter - 1;
        this.protocol = protocol;
        this.readPrepareStmtResult = readPrepareStmtResult;
        this.bulkSend = bulkSend;
        this.paramCount = paramCount;
        this.binaryProtocol = binaryProtocol;
        this.results = results;
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
            } catch (SQLException queryException) {
                asyncMultiReadResult.setException(queryException);
            }
        }

        //read all corresponding results
        int counter = 0;

        //ensure to not finished loop while all bulk has not been send
        while (!status.sendEnded || counter < status.sendSubCmdCounter) {
            //read results for each send data
            while (counter < status.sendSubCmdCounter) {
                try {
                    protocol.getResult(results);
                } catch (SQLException qex) {
                    if (asyncMultiReadResult.getException() == null) {
                        asyncMultiReadResult.setException(bulkSend.handleResultException(qex, results,
                                parametersList, queries, counter, sendCmdInitialCounter, paramCount,
                                asyncMultiReadResult.getPrepareResult()));
                    }
                }
                counter++;

                if (Thread.currentThread().isInterrupted()) {
                    asyncMultiReadResult.setException(new SQLException("Interrupted reading responses ", INTERRUPTED_EXCEPTION.getSqlState(), -1));
                    break;
                }
            }
        }

        if (protocol.getOptions().socketTimeout != null) {
            protocol.changeSocketSoTimeout(protocol.getOptions().socketTimeout);
        }

        return asyncMultiReadResult;
    }

}
