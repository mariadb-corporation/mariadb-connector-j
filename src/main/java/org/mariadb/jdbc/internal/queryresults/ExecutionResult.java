package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;

import java.sql.SQLException;
import java.sql.Statement;

public abstract class ExecutionResult {
    protected MariaSelectResultSet result = null;
    private Statement statement = null;
    private boolean moreResultAvailable;
    private int fetchSize;
    private boolean selectPossible;

    /**
     * Constructor.
     *
     * @param statement current statement
     * @param fetchSize execution fetch size
     * @param selectPossible is select query possible ?
     */
    public ExecutionResult(Statement statement, int fetchSize, boolean selectPossible) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
    }

    public MariaSelectResultSet getResult() {
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

    public abstract void addResult(MariaSelectResultSet result, boolean moreResultAvailable);

    public abstract void addStats(long affectedRows, long insertId, boolean moreResultAvailable);

    public abstract long[] getInsertIds();

    public abstract boolean hasMoreThanOneAffectedRows();

    public abstract int getFirstAffectedRows();

    public abstract void addStatsError();

    public boolean isSelectPossible() {
        return selectPossible;
    }
}

