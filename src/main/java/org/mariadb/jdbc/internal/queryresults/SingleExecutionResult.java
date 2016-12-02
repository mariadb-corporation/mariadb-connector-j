package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class SingleExecutionResult implements ExecutionResult {

    private MariaSelectResultSet result = null;
    private MariaDbStatement statement = null;
    private boolean moreResultAvailable;
    private int fetchSize;
    private boolean selectPossible;
    private boolean canHaveCallableResultSet;
    private Deque<ExecutionResult> cachedExecutionResults;
    private long insertId;
    private long affectedRows = -1;

    /**
     * Default constructor.
     *
     * @param statement                current statement
     * @param fetchSize                fetch size
     * @param selectPossible           select result possible
     * @param canHaveCallableResultSet can be callablestatement
     */
    public SingleExecutionResult(MariaDbStatement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultSet) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultSet = canHaveCallableResultSet;
    }

    /**
     * Constructor with possible multiple results.
     *
     * @param statement                current statement
     * @param fetchSize                fetch size
     * @param selectPossible           select result possible
     * @param canHaveCallableResultSet can be callablestatement
     * @param canHaveMoreResults       tell that results may have multiple resultset
     */
    public SingleExecutionResult(MariaDbStatement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultSet,
                                 boolean canHaveMoreResults) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultSet = canHaveCallableResultSet;
        this.cachedExecutionResults = new ArrayDeque<>();
    }

    /**
     * Constructor with affected rows and insertIds.
     *
     * @param statement                current statement
     * @param fetchSize                fetch size
     * @param selectPossible           select result possible
     * @param canHaveCallableResultSet can be callablestatement
     * @param affectedRows             affected rows
     * @param insertId                 insert id (auto generated)
     */
    public SingleExecutionResult(MariaDbStatement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultSet,
                                 long affectedRows, long insertId) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultSet = canHaveCallableResultSet;
        this.insertId = insertId;
        this.affectedRows = affectedRows;
    }

    /**
     * Constructor with additional Resultset.
     *
     * @param statement                current statement
     * @param fetchSize                fetch size
     * @param selectPossible           select result possible
     * @param canHaveCallableResultSet can be callablestatement
     * @param result                   resultset
     */
    public SingleExecutionResult(MariaDbStatement statement, int fetchSize, boolean selectPossible, boolean canHaveCallableResultSet,
                                 MariaSelectResultSet result) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.selectPossible = selectPossible;
        this.canHaveCallableResultSet = canHaveCallableResultSet;
        this.result = result;
        this.insertId = 0;
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
        this.insertId = 0;
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

    public int getFirstAffectedRows() {
        return (int) affectedRows;
    }

    public void addStatsError(boolean moreResultAvailable) {
        this.affectedRows = Statement.EXECUTE_FAILED;
        setMoreResultAvailable(moreResultAvailable);
    }

    public void fixStatsError(int sendCommand) {
    }

    public MariaSelectResultSet getResultSet() {
        return result;
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
        if (result != null) {
            result.close();
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

    /**
     * Return auto_increment keys in resultSet.
     *
     * @param autoIncrementIncrement connection autoIncrementIncrement variable value
     * @param protocol current protocol
     * @return resultSet
     */
    public ResultSet getGeneratedKeys(int autoIncrementIncrement, Protocol protocol) {
        if (result == null) {
            ColumnInformation[] columns = new ColumnInformation[1];
            columns[0] = ColumnInformation.create(INSERT_ID_ROW_NAME, MariaDbType.BIGINT);

            List<byte[][]> rows = new ArrayList<>();

            //multi insert in one execution. will create result based on autoincrement
            if (affectedRows > 1) {
                for (int i = 0; i < affectedRows; i++) {
                    if (insertId != 0) {
                        byte[][] row = {String.valueOf(insertId  + i * autoIncrementIncrement).getBytes()};
                        rows.add(row);
                    }
                }
            } else {
                if (insertId != 0) {
                    byte[][] row = {String.valueOf(insertId).getBytes()};
                    rows.add(row);
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
