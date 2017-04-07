package org.mariadb.jdbc.internal.protocol;

import java.io.IOException;
import java.util.List;

import org.mariadb.jdbc.internal.packet.ComStmtPrepare;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.Results;
import org.mariadb.jdbc.internal.util.BulkStatus;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;

/**
 * Aurora isn't compatible with useBatchMultiSend (https://jira.mariadb.org/browse/CONJ-386)
 * 
 * Even though this option is hard coded for HaMode.AURORA, batchExecute code flow didn't handle it appropriately.
 * In particular, AbstractQueryProtocol.readScheduler will be null if useBatchMultiSend=false.
 * 
 * Modified executeBatchStandard() to call this utility class in case useBatchMultiSend is turned off.
 * 
 * @author koh
 *
 */
public class SyncReadUtil {
	public static void readResultSynchronously(ComStmtPrepare comStmtPrepare, BulkStatus status,
            Protocol protocol, boolean readPrepareStmtResult, AbstractMultiSend bulkSend, int paramCount,
            boolean binaryProtocol, Results results,
            List<ParameterHolder[]> parametersList, List<String> queries, PrepareResult prepareResult,
            AsyncMultiReadResult syncReadResult) throws IOException, QueryException {
		if (!protocol.getOptions().useBatchMultiSend || AbstractQueryProtocol.readScheduler == null) {

			if (readPrepareStmtResult) {
				try {
					syncReadResult.setPrepareResult(comStmtPrepare.read(protocol.getPacketFetcher()));
				} catch (QueryException queryException) {
					syncReadResult.setException(queryException);
				}
			}

			try {
				protocol.getResult(results);
			} catch (QueryException qex) {
				if (syncReadResult.getException() == null) {
					syncReadResult.setException(bulkSend.handleResultException(qex, results, parametersList, queries, status.sendCmdCounter, status.sendCmdCounter - 1, paramCount, syncReadResult.getPrepareResult()));
				}
			}
		}
	}
}
