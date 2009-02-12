package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.packet.FieldPacket;

import java.util.*;

/**
 * TODO: refactor, badly need to split this into two/three different classes, one for insert/update/ddl, one for selects and one for generated keys?
 *
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 8:15:55 PM
 */
public class DrizzleQueryResult implements QueryResult {
    private DrizzleQueryResult generatedKeysResult;

    // TODO: tagged class antipattern...
    public enum QueryResultType {
        SELECT, MODIFY, GENERATEDKEYS
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
    public DrizzleQueryResult(long insertId) {
        queryResultType= QueryResultType.GENERATEDKEYS;
        fieldPackets=null;
        rowCounter=-1;
        if(insertId>0) {
            List<String> genKeyList = new ArrayList<String>();
            genKeyList.add(String.valueOf(insertId));
            resultSet.add(genKeyList);
            columnNameMap.put("insert_id",0);
        }
    }
    public boolean next() {
        if(queryResultType == QueryResultType.MODIFY)
            return false;
        rowCounter++;
        return rowCounter < resultSet.size();
    }

    public void close() {
        this.resultSet=null;
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
        this.generatedKeysResult = new DrizzleQueryResult(insertId);
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
    public DrizzleQueryResult getGeneratedKeysResult() {
        return generatedKeysResult;
    }

}