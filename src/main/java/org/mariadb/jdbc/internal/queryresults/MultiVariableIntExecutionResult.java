package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class MultiVariableIntExecutionResult implements MultiExecutionResult {

    public Deque<ExecutionResult> cachedExecutionResults;
    private MariaDbStatement statement = null;
    private boolean moreResultAvailable;
    private int fetchSize;
    private boolean selectPossible;
    private boolean canHaveCallableResultset;
    private MariaSelectResultSet resultSet = null;
    private List<Long> insertIds;
    private List<Integer> affectedRows;

    /**
     * Constructor. Creating resultSet data with size according to datas.
     *
     * @param statement      current statement
     * @param size           initial data size
     * @param fetchSize      resultSet fetch size
     * @param selectPossible is select command possible
     */
    public MultiVariableIntExecutionResult(MariaDbStatement statement, int size, int fetchSize, boolean selectPossible) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultset = false;
        this.cachedExecutionResults = new ArrayDeque<>();
        affectedRows = new ArrayList<>(size);
        insertIds = new ArrayList<>(size);
    }

    /**
     * Add a resultSet information.
     *
     * @param result              resultset implementation
     * @param moreResultAvailable is there additional packet
     */
    public void addResultSet(MariaSelectResultSet result, boolean moreResultAvailable) {
        this.resultSet = result;
        this.insertIds.add(0L);
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
        this.insertIds.add(insertId);
        this.affectedRows.add((int) affectedRows);
        setMoreResultAvailable(moreResultAvailable);
    }

    /**
     * Get update array.
     *
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

    public int getFirstAffectedRows() {
        return affectedRows.get(0);
    }

    /**
     * Add resutl information when an Exception occur during batch.
     *
     * @param moreResultAvailable has more result flag
     */
    public void addStatsError(boolean moreResultAvailable) {
        this.affectedRows.add(Statement.EXECUTE_FAILED);
        this.insertIds.add(0L);
        setMoreResultAvailable(moreResultAvailable);
    }

    public void addStatsError() {
        this.affectedRows.add(Statement.EXECUTE_FAILED);
        this.insertIds.add(0L);
    }

    /**
     * Add missing information when Exception is thrown.
     *
     * @param sendCommand send number of command
     */
    public void fixStatsError(int sendCommand) {
        for (; this.affectedRows.size() < sendCommand; ) {
            this.affectedRows.add(Statement.EXECUTE_FAILED);
            this.insertIds.add(0L);
        }
    }

    /**
     * Set resultSet for rewrite queries.
     * <p>
     * INSERT INTO XX VALUES (YYY)
     * INSERT INTO XX VALUES (ZZZ)
     * is rewritten
     * INSERT INTO XX VALUES (YYY), (ZZZ)
     * <p>
     * so modified row, will all be on the first row, or on a few rows :
     * queries will split to have query size under the max_allowed_size, so data can be on multiple rows
     *
     * @param expectedSize   batchSize
     * @param hasException has exception
     * @return affected rows
     */
    public int[] updateResultsForRewrite(int expectedSize, boolean hasException) {
        long totalAffectedRows = 0;
        Iterator<Integer> iterator = affectedRows.iterator();

        while (iterator.hasNext()) {
            totalAffectedRows += iterator.next().intValue();
        }
        int realSize = (int) Math.max(expectedSize, totalAffectedRows);
        int baseResult = totalAffectedRows == realSize ? 1 : Statement.SUCCESS_NO_INFO;

        int[] arr = new int[realSize];
        int counter = 0;
        iterator = affectedRows.iterator();
        while (iterator.hasNext()) {
            int affectedRow = iterator.next().intValue();
            for (int i = 0; i < affectedRow; i++) arr[counter++] = baseResult;
        }
        for (; counter < realSize; ) {
            arr[counter++] = hasException ? Statement.EXECUTE_FAILED : Statement.SUCCESS_NO_INFO;
        }
        return arr;
    }

    /**
     * Set update resultSet right on multiple rewrite.
     * <p>
     * INSERT XXXX
     * INSERT XXXX
     * is rewritten
     * INSERT XXXX;INSERT XXXX
     * <p>
     * So affected rows and insert Id are separate in as many okPacket.
     *
     * @param waitedSize   batchSize
     * @param hasException has exception
     */
    public int[] updateResultsMultiple(int waitedSize, boolean hasException) {
        if (hasException) {
            for (int i = affectedRows.size(); i < waitedSize; i++) {
                addStatsError();
            }
        }
        return getAffectedRows();
    }

    public MariaSelectResultSet getResultSet() {
        return resultSet;
    }

    public MariaDbStatement getStatement() {
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

    public boolean isCanHaveCallableResultSet() {
        return canHaveCallableResultset;
    }

    public Deque<ExecutionResult> getCachedExecutionResults() {
        return cachedExecutionResults;
    }

    /**
     * Return auto_increment keys in resultSet.
     *
     * @param autoIncrementIncrement connection autoIncrementIncrement variable value
     * @param protocol current protocol
     * @return resultSet
     */
    public ResultSet getGeneratedKeys(int autoIncrementIncrement, Protocol protocol) {
        if (resultSet == null) {
            ColumnInformation[] columns = new ColumnInformation[1];
            columns[0] = ColumnInformation.create(INSERT_ID_ROW_NAME, MariaDbType.BIGINT);
            List<byte[][]> rows = new ArrayList<>();

            //multi insert in one execution. will create result based on autoincrement
            if (affectedRows.size() > 0 && affectedRows.get(0) > 1) {


                Iterator<Integer> iterator = affectedRows.iterator();
                Iterator<Long> idsIterator = insertIds.iterator();
                while (iterator.hasNext()) {
                    int affectedRow = iterator.next().intValue();
                    if (affectedRow != Statement.EXECUTE_FAILED) {
                        if (idsIterator.hasNext()) {
                            Long insertId = idsIterator.next().longValue();
                            if (insertId != null && insertId != 0) {
                                for (int i = 0; i < affectedRow; i++) {
                                    byte[][] row = {String.valueOf(insertId + i * autoIncrementIncrement).getBytes()};
                                    rows.add(row);
                                }
                            }
                        }
                    }
                }

            } else {
                for (Long insertId : insertIds) {
                    if (insertId != null && insertId != 0) {
                        byte[][] row = {String.valueOf(insertId).getBytes()};
                        rows.add(row);
                    }
                }
            }
            return new MariaSelectResultSet(columns, rows, protocol, ResultSet.TYPE_SCROLL_SENSITIVE) {
                @Override
                public int findColumn(String name) {
                    return 1;
                }
            };
        }
        return MariaSelectResultSet.EMPTY;
    }
}
