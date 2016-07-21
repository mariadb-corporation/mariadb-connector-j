package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;

public class SingleExecutionResult implements ExecutionResult {

    private MariaSelectResultSet result = null;
    private Statement statement = null;
    private boolean moreResultAvailable;
    private int fetchSize;
    private boolean selectPossible;
    private boolean canHaveCallableResultset;
    private Deque<ExecutionResult> cachedExecutionResults;
    private long insertId;
    private long affectedRows = -1;

    /**
     * Default constructor.
     *
     * @param statement current statement
     * @param fetchSize fetch size
     * @param selectPossible select result possible
     * @param canHaveCallableResultset can be callablestatement
     */
    public SingleExecutionResult(Statement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultset) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = canHaveCallableResultset;
    }

    /**
     * Constructor with possible multiple results.
     *
     * @param statement current statement
     * @param fetchSize fetch size
     * @param selectPossible select result possible
     * @param canHaveCallableResultset can be callablestatement
     * @param canHaveMoreResults tell that results may have multiple resultset
     */
    public SingleExecutionResult(Statement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultset,
                                 boolean canHaveMoreResults) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = canHaveCallableResultset;
        this.cachedExecutionResults = new ArrayDeque<>();
    }

    /**
     * Constructor with affected rows and insertIds.
     *
     * @param statement current statement
     * @param fetchSize fetch size
     * @param selectPossible select result possible
     * @param canHaveCallableResultset can be callablestatement
     * @param affectedRows affected rows
     * @param insertId insert id (auto generated)
     */
    public SingleExecutionResult(Statement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultset,
                                 long affectedRows, long insertId) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = canHaveCallableResultset;
        this.insertId = insertId;
        this.affectedRows = affectedRows;
    }

    /**
     * Constructor with additional Resultset.
     * @param statement current statement
     * @param fetchSize fetch size
     * @param selectPossible select result possible
     * @param canHaveCallableResultset can be callablestatement
     * @param result resultset
     */
    public SingleExecutionResult(Statement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultset,
                                 MariaSelectResultSet result) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = canHaveCallableResultset;
        this.result = result;
        this.insertId = Statement.SUCCESS_NO_INFO;
        this.affectedRows = -1;
    }

    /**
     * Add a resultSet information.
     *
     * @param result              resultset implementation
     * @param moreResultAvailable is there additional packet
     */
    public void addResultSet(MariaSelectResultSet result, boolean moreResultAvailable) {
        this.result = result;
        this.insertId = Statement.SUCCESS_NO_INFO;
        this.affectedRows = -1;
        this.setMoreResultAvailable(moreResultAvailable);
    }

    /**
     * Add execution statistics.
     *
     * @param affectedRows        number of affected rows
     * @param insertId            primary key
     * @param moreResultAvailable is there additional packet
     */
    public void addStats(long affectedRows, long insertId, boolean moreResultAvailable) {
        this.insertId = insertId;
        this.affectedRows = affectedRows;
        setMoreResultAvailable(moreResultAvailable);
    }

    public long[] getInsertIds() {
        return new long[]{insertId};
    }

    public long getInsertId() {
        return insertId;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public boolean hasMoreThanOneAffectedRows() {
        return affectedRows > 1;
    }


    public int getFirstAffectedRows() {
        return (int) affectedRows;
    }

    public void addStatsError() {
        ;
    }

    public MariaSelectResultSet getResultSet() {
        return result;
    }

    public Statement getStatement() {
        return statement;
    }

    public boolean hasMoreResultAvailable() {
        return moreResultAvailable;
    }

    protected void setMoreResultAvailable(boolean moreResultAvailable) {
        this.moreResultAvailable = moreResultAvailable;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * Close resultset if needed.
     *
     * @throws SQLException if exception occur during resultset close.
     */
    public void close() throws SQLException {
        if (result != null) {
            result.close();
        }
    }

    public boolean isSelectPossible() {
        return selectPossible;
    }

    public boolean isCanHaveCallableResultset() {
        return canHaveCallableResultset;
    }

    public Deque<ExecutionResult> getCachedExecutionResults() {
        return cachedExecutionResults;
    }

    public boolean isSingleExecutionResult() {
        return true;
    }
}
