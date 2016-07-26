package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.Statement;

public class SingleExecutionResult extends ExecutionResult {

    long insertId;
    long affectedRows;

    public SingleExecutionResult(Statement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultset) {
        super(statement, fetchSize, selectPossible, canHaveCallableResultset);
    }

    /**
     * Add a resultSet information.
     *
     * @param result              resultset implementation
     * @param moreResultAvailable is there additional packet
     */
    public void addResult(MariaSelectResultSet result, boolean moreResultAvailable) {
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

    public void addStatsError() { }

}
