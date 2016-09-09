package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class MultiFixedIntExecutionResult implements MultiExecutionResult {

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
    public MultiFixedIntExecutionResult(Statement statement, int size, int fetchSize, boolean selectPossible) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = false;
        this.cachedExecutionResults = new ArrayDeque<>();

        affectedRows = new int[size];
        insertId = new long[size];
        Arrays.fill(affectedRows, Statement.EXECUTE_FAILED);
        Arrays.fill(insertId, Statement.EXECUTE_FAILED);
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
     *
     * @param waitedSize waited size
     * @param hasException has exception
     */
    public int[] updateResultsForRewrite(int waitedSize, boolean hasException) {
        return null;
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
     * @param waitedSize  batchSize
     * @param hasException has exception
     */
    public int[] updateResultsMultiple(int waitedSize, boolean hasException) {
        return null;
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

    public boolean isSingleExecutionResult() {
        return false;
    }

}
