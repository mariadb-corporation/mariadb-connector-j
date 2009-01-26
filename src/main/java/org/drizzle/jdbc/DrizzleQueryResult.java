package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.FieldPacket;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 8:15:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleQueryResult {
    private final List<FieldPacket> fieldPackets;
    private List<List<String>> resultSet = new ArrayList<List<String>>();
    private Map<String, Integer> columnNameMap = new HashMap<String,Integer>();
    private int rowCounter;

    public DrizzleQueryResult(List<FieldPacket> fieldPackets) {
        this.fieldPackets=fieldPackets;
        rowCounter=-1;
        int i=0;        
        for(FieldPacket fp : fieldPackets) {
            columnNameMap.put(fp.getColumnName().toLowerCase(),i++);
        }
    }
    
    public DrizzleQueryResult() {
        fieldPackets = null;
        rowCounter=-1;
    }

    public boolean next() {
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

    public int getRows() {
        return resultSet.size();
    }
}