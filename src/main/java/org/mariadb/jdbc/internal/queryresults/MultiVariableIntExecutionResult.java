package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class MultiVariableIntExecutionResult implements MultiExecutionResult {

    private Statement statement = null;
    private boolean moreResultAvailable;
    private int fetchSize;
    private boolean selectPossible;
    private boolean canHaveCallableResultset;
    public Deque<ExecutionResult> cachedExecutionResults;
    private MariaSelectResultSet resultSet = null;
    private List<Long> insertId;
    private List<Integer> affectedRows;

    /**
     * Constructor. Creating resultSet data with size according to datas.
     *
     * @param statement current statement
     * @param size      initial data size
     * @param fetchSize resultet fetch size
     * @param selectPossible is select command possible
     */
    public MultiVariableIntExecutionResult(Statement statement, int size, int fetchSize, boolean selectPossible) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = false;
        this.cachedExecutionResults = new ArrayDeque<>();

        affectedRows = new ArrayList<>(size);
        insertId = new ArrayList<>(size);
    }

    /**
     * Add a resultSet information.
     *
     * @param result              resultset implementation
     * @param moreResultAvailable is there additional packet
     */
    public void addResultSet(MariaSelectResultSet result, boolean moreResultAvailable) {
        this.resultSet = result;
        this.insertId.add((long) Statement.SUCCESS_NO_INFO);
        this.affectedRows.add(-1);
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
        this.insertId.add(insertId);
        this.affectedRows.add((int) affectedRows);
        setMoreResultAvailable(moreResultAvailable);
    }

    /**
     * Get insert ids.
     * @return insert ids results
     */
    public long[] getInsertIds() {
        long[] ret = new long[insertId.size()];
        Iterator<Long> iterator = insertId.iterator();
        for (int i = 0; i < ret.length; i++) {
            ret[i] = iterator.next().longValue();
        }
        return ret;
    }

    /**
     * Get update array.
     * @return update array.
     */
    public int[] getAffectedRows() {
        int[] ret = new int[affectedRows.size()];
        Iterator<Integer> iterator = affectedRows.iterator();
        for (int i = 0; i < ret.length; i++) {
            ret[i] = iterator.next().intValue();
        }
        return ret;
    }

    public boolean hasMoreThanOneAffectedRows() {
        return affectedRows.size() > 0;
    }

    public int getFirstAffectedRows() {
        return affectedRows.get(0);
    }

    public void addStatsError() {
        this.insertId.add(new Long(Statement.EXECUTE_FAILED));
        this.affectedRows.add(Statement.EXECUTE_FAILED);
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
     * @param waitedSize  batchSize
     * @param hasException has exception
     */
    public void updateResultsForRewrite(int waitedSize, boolean hasException) {
        long totalAffectedRows = 0;
        Iterator<Integer> iterator = affectedRows.iterator();

        while (iterator.hasNext()) {
            totalAffectedRows += iterator.next().intValue();
        }
        int realSize = Math.max(waitedSize, affectedRows.size());
        int resultVal = hasException ? Statement.EXECUTE_FAILED : (totalAffectedRows == realSize ? 1 : Statement.SUCCESS_NO_INFO);

        Integer[] arr = new Integer[realSize];
        Arrays.fill(arr, resultVal);
        affectedRows = Arrays.asList(arr);
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
    public void updateResultsMultiple(int waitedSize, boolean hasException) {
        if (hasException) {
            for (int i = affectedRows.size() ; i < waitedSize; i++) {
                addStatsError();
            }
        }

//        if (!cachedExecutionResults.isEmpty()) {
//            //was rewrite with multiple insert
//            int[] newAffectedRows = new int[affectedRows.length + cachedExecutionResults.size()];
//            long[] newInsertIds = new long[insertId.length + cachedExecutionResults.size()];
//            int counter = 0;
//            for (; counter < affectedRows.length; counter++) {
//                newAffectedRows[counter] = affectedRows[counter];
//                newInsertIds[counter] = insertId[counter];
//            }
//            SingleExecutionResult executionResult;
//            while ((executionResult = (SingleExecutionResult) cachedExecutionResults.poll()) != null) {
//                newAffectedRows[counter] = (int) executionResult.getAffectedRows();
//                newInsertIds[counter++] = executionResult.getInsertId();
//            }
//            affectedRows = newAffectedRows;
//            insertId = newInsertIds;
//        }
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
