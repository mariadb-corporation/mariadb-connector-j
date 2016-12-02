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

public class MultiFixedIntExecutionResult implements MultiExecutionResult {

    public Deque<ExecutionResult> cachedExecutionResults;
    private MariaDbStatement statement = null;
    private boolean moreResultAvailable;
    private int fetchSize;
    private boolean selectPossible;
    private boolean canHaveCallableResultSet;
    private MariaSelectResultSet resultSet = null;
    private long[] insertIds;
    private int[] affectedRows;
    private int currentStat = 0;

    /**
     * Constructor. Creating resultSet data with size according to datas.
     *
     * @param statement      current statement
     * @param size           data size
     * @param fetchSize      resultet fetch size
     * @param selectPossible is select command possible
     */
    public MultiFixedIntExecutionResult(MariaDbStatement statement, int size, int fetchSize, boolean selectPossible) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultSet = false;
        this.cachedExecutionResults = new ArrayDeque<>();

        affectedRows = new int[size];
        insertIds = new long[size];
        Arrays.fill(affectedRows, Statement.EXECUTE_FAILED);
        Arrays.fill(insertIds, 0);
    }

    /**
     * Add a resultSet information.
     *
     * @param result              resultset implementation
     * @param moreResultAvailable is there additional packet
     */
    public void addResultSet(MariaSelectResultSet result, boolean moreResultAvailable) {
        this.resultSet = result;
        this.insertIds[currentStat] = 0;
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
        this.insertIds[currentStat] = insertId;
        this.affectedRows[currentStat++] = (int) affectedRows;
        setMoreResultAvailable(moreResultAvailable);
    }

    public int[] getAffectedRows() {
        return affectedRows;
    }

    public int getFirstAffectedRows() {
        return affectedRows[0];
    }

    public void addStatsError(boolean moreResultAvailable) {
        this.affectedRows[currentStat++] = Statement.EXECUTE_FAILED;
        setMoreResultAvailable(moreResultAvailable);
    }

    /**
     * Add missing information when Exception is thrown.
     *
     * @param sendCommand send number of command
     */
    public void fixStatsError(int sendCommand) {
        for (; this.affectedRows.length < sendCommand; ) {
            this.affectedRows[currentStat++] = Statement.EXECUTE_FAILED;
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
     * @param waitedSize   waited size
     * @param hasException has exception
     */
    public int[] updateResultsForRewrite(int waitedSize, boolean hasException) {
        return null;
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
        return null;
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
        return canHaveCallableResultSet;
    }

    public Deque<ExecutionResult> getCachedExecutionResults() {
        return cachedExecutionResults;
    }

    public int getCurrentStat() {
        return currentStat;
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
            if (affectedRows.length > 0 && affectedRows[0] > 1) {
                for (int affectedRow : affectedRows) {
                    for (int counter = 0; counter < affectedRow; counter++) {
                        for (long insertId : insertIds) {
                            byte[][] row = {String.valueOf(insertId + counter * autoIncrementIncrement).getBytes()};
                            rows.add(row);
                        }
                    }
                }
            } else {
                for (long insertId : insertIds) {
                    if (insertId != 0) {
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
