package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;

public class MultiIntExecutionResult implements ExecutionResult {

    private Statement statement = null;
    private boolean moreResultAvailable;
    private int fetchSize;
    private boolean selectPossible;
    private boolean canHaveCallableResultset;
    public Deque<ExecutionResult> cachedExecutionResults;
    private MariaSelectResultSet resultSet = null;
    private long[] insertId;
    private int[] affectedRows;
    private int currentStat = 0;

    /**
     * Constructor. Creating resultSet data with size according to datas.
     *
     * @param statement current statement
     * @param size      data size
     * @param fetchSize resultet fetch size
     * @param selectPossible is select command possible
     */
    public MultiIntExecutionResult(Statement statement, int size, int fetchSize, boolean selectPossible) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = false;
        this.cachedExecutionResults = new ArrayDeque<>();

        affectedRows = new int[size];
        insertId = new long[size];
    }

    /**
     * Add a resultSet information.
     *
     * @param result              resultset implementation
     * @param moreResultAvailable is there additional packet
     */
    public void addResultSet(MariaSelectResultSet result, boolean moreResultAvailable) {
        this.resultSet = result;
        this.insertId[currentStat] = Statement.SUCCESS_NO_INFO;
        this.affectedRows[currentStat++] = -1;
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
        this.insertId[currentStat] = insertId;
        this.affectedRows[currentStat++] = (int) affectedRows;
        setMoreResultAvailable(moreResultAvailable);
    }

    public long[] getInsertIds() {
        return insertId;
    }

    public int[] getAffectedRows() {
        return affectedRows;
    }

    public boolean hasMoreThanOneAffectedRows() {
        return affectedRows.length > 0 && affectedRows[0] > 1;
    }

    public int getFirstAffectedRows() {
        return affectedRows[0];
    }

    public void addStatsError() {
        this.insertId[currentStat] = Statement.EXECUTE_FAILED;
        this.affectedRows[currentStat++] = Statement.EXECUTE_FAILED;
    }

    /**
     * Set resultSet for rewrite queries.
     *
     * INSERT INTO XX VALUES (YYY)
     * INSERT INTO XX VALUES (ZZZ)
     * is rewritten
     * INSERT INTO XX VALUES (YYY), (ZZZ)
     *
     * so modified row, will all be on the first row, or on a few rows :
     * queries will split to have query size under the max_allowed_size, so data can be on multiple rows
     */
    public void updateResultsForRewrite() {
        long totalAffectedRows = 0;
        int row = 0;
        while (row < affectedRows.length && affectedRows[row] > 0) {
            totalAffectedRows += affectedRows[row++];
        }
        int resultVal = totalAffectedRows == affectedRows.length ? 1 : Statement.SUCCESS_NO_INFO;
        for (row = 0; row < affectedRows.length; row++) {
            affectedRows[row] = resultVal;
        }
    }

    /**
     * Set update resultSet right on multiple rewrite.
     *
     * INSERT XXXX
     * INSERT XXXX
     * is rewritten
     * INSERT XXXX;INSERT XXXX
     *
     * So affected rows and insert Id are separate in as many okPacket.
     *
     * @param cachedExecutionResults other okPacket.
     */
    public void updateResultsMultiple(Deque<ExecutionResult> cachedExecutionResults) {
        for (int i = 1 ; i < affectedRows.length; i++) {
            SingleExecutionResult executionResult = (SingleExecutionResult) cachedExecutionResults.poll();
            affectedRows[i] = (int) executionResult.getAffectedRows();
            insertId[i] = executionResult.getInsertId();
        }

        if (!cachedExecutionResults.isEmpty()) {
            //was rewrite with multiple insert
            int[] newAffectedRows = new int[affectedRows.length + cachedExecutionResults.size()];
            long[] newInsertIds = new long[insertId.length + cachedExecutionResults.size()];
            int counter = 0;
            for (; counter < affectedRows.length; counter++) {
                newAffectedRows[counter] = affectedRows[counter];
                newInsertIds[counter] = insertId[counter];
            }
            SingleExecutionResult executionResult;
            while ((executionResult = (SingleExecutionResult) cachedExecutionResults.poll()) != null) {
                newAffectedRows[counter] = (int) executionResult.getAffectedRows();
                newInsertIds[counter++] = executionResult.getInsertId();
            }
            affectedRows = newAffectedRows;
            insertId = newInsertIds;
        }
    }

    public MariaSelectResultSet getResultSet() {
        return resultSet;
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
        if (resultSet != null) {
            resultSet.close();
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

    public void addResult(ExecutionResult executionResult) {
        cachedExecutionResults.add(executionResult);
    }

}
