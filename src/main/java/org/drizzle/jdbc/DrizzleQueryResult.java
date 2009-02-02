package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.FieldPacket;

import java.util.*;

/**
 * TODO: refactor, badly need to split this into two different classes, one for insert/update/ddl and one for selects
 *
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 8:15:55 PM
 */
public class DrizzleQueryResult {
    public enum QueryResultType {
        SELECT, MODIFY
    }

    private final List<FieldPacket> fieldPackets;
    private List<List<String>> resultSet = new ArrayList<List<String>>();
    private Map<String, Integer> columnNameMap = new HashMap<String,Integer>();
    private int rowCounter;
    private int updateCount;
    private long insertId;
    private int warnings;
    private String message;
    private final QueryResultType queryResultType;
    /**
     * Create a new query result for a SELECT
     * @param fieldPackets
     */
    public DrizzleQueryResult(List<FieldPacket> fieldPackets) {
        queryResultType = QueryResultType.SELECT;
        this.fieldPackets=fieldPackets;
        rowCounter=-1;
        int i=0;        
        for(FieldPacket fp : fieldPackets) {
            columnNameMap.put(fp.getColumnName().toLowerCase(),i++);
        }
    }
    
    public DrizzleQueryResult() {
        queryResultType = QueryResultType.MODIFY;
        fieldPackets = null;
        rowCounter=-1;
    }

    public boolean next() {
        if(queryResultType == QueryResultType.MODIFY)
            throw new IllegalStateException("No result set for this query");
        rowCounter++;
        if(rowCounter < resultSet.size()) {
            return true;
        }
        return false;
    }

    public void addRow(List<String> row) {
        resultSet.add(row);
    }

    public List<FieldPacket> getFieldPackets() {
        return fieldPackets;
    }
    
    public String getString(int i) {
        String result = resultSet.get(rowCounter).get(i-1);
        if(result==null)
            return "NULL";
        return result;
    }

    public String getString(String column) {
        return getString(columnNameMap.get(column.toLowerCase())+1);
    }

    public void setUpdateCount(int updateCount) {
        this.updateCount=updateCount;
    }
    
    public int getUpdateCount() {
        return updateCount;
    }
    
    public void setInsertId(long insertId) {
        this.insertId = insertId;
    }

    public long getInsertId() {
        return insertId;
    }
    
    public int getRows() {
        return resultSet.size();
    }

    public void setWarnings(int warnings) {
        this.warnings = warnings;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }
}