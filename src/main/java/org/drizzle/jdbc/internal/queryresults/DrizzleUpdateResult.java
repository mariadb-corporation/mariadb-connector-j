package org.drizzle.jdbc.internal.queryresults;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:20:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleUpdateResult implements ModifyQueryResult {
    private final long updateCount;
    private final short warnings;
    private final String message;
    private final long insertId;
    private final QueryResult generatedKeysResult;

    public DrizzleUpdateResult(long updateCount, short warnings, String message, long insertId) {
        this.updateCount = updateCount;
        this.warnings=warnings;
        this.message=message;
        this.insertId=insertId;
        generatedKeysResult = new DrizzleInsertIdQueryResult(insertId);
    }

    public long getUpdateCount() {
        return updateCount;
    }

    public ResultSetType getResultSetType() {
        return ResultSetType.MODIFY;
    }

    public void close() {
    }

    public short getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }

    public List<ColumnInformation> getColumnInformation() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getRows() {
        return 0; 
    }

    public long getInsertId() {
        return insertId;
    }

    public QueryResult getGeneratedKeysResult() {
        return generatedKeysResult;
    }
}
