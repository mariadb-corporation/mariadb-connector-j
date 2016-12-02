package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;

public interface ExecutionResult {
    String INSERT_ID_ROW_NAME = "insert_id";

    MariaSelectResultSet getResultSet();

    MariaDbStatement getStatement();

    boolean hasMoreResultAvailable();

    int getFetchSize();

    void setFetchSize(int fetchSize);

    /**
     * Close resultset if needed.
     *
     * @throws SQLException if exception occur during resultset close.
     */
    void close() throws SQLException;

    void addResultSet(MariaSelectResultSet result, boolean moreResultAvailable);

    void addStats(long affectedRows, long insertId, boolean moreResultAvailable);

    void addStatsError(boolean moreResultAvailable);

    void fixStatsError(int sendCommand);

    int getFirstAffectedRows();

    boolean isSelectPossible();

    boolean isCanHaveCallableResultSet();

    Deque<ExecutionResult> getCachedExecutionResults();

    ResultSet getGeneratedKeys(int autoIncrementIncrement, Protocol protocol);
}

