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
    private List<List<ValueObject>> resultSet = new ArrayList<List<ValueObject>>();
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
            List<ValueObject> genKeyList = new ArrayList<ValueObject>();
            genKeyList.add(DrizzleValueObject.fromLong(insertId));
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

    public void addRow(List<ValueObject> row) {
        resultSet.add(row);
    }

    public List<FieldPacket> getFieldPackets() {
        return fieldPackets;
    }

    /**
     * gets the value at position i in the result set. i starts at zero!
     * @param i index, starts at 0
     * @return
     */
    public ValueObject getValueObject(int i) {
        return resultSet.get(rowCounter).get(i);
    }

    public ValueObject getValueObject(String column) {
        return getValueObject(columnNameMap.get(column.toLowerCase()));
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

    public int getColumnId(String columnLabel) {
        return columnNameMap.get(columnLabel.toLowerCase());
    }

    public int getRowPointer() {
        return rowCounter;
    }

    public void moveRowPointerTo(int i) {
        this.rowCounter=i;    
    }

}