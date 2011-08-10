package org.skysql.jdbc.internal.common.queryresults;

import org.skysql.jdbc.internal.common.ColumnInformation;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ColumnNameMap {
    Map<String, Integer> map;
    Map<String, Integer> labelMap;
    List<ColumnInformation> columnInfo;

    public ColumnNameMap(List<ColumnInformation> columnInfo) {
       this.columnInfo = columnInfo;
    }

    public int getIndex(String name) throws SQLException {

        if (columnInfo == null) {
           throw new SQLException("No such column :" + name);
        }
        if (map == null) {
            map = new HashMap<String, Integer>();
            int i=0;
            for(ColumnInformation ci : columnInfo) {
                String columnName = ci.getOriginalName().toLowerCase();
                if (columnName.equals("")) {
                    // for name-less columns (there CAN be some), use their alias
                    columnName = ci.getName().toLowerCase();
                }
                map.put(columnName, i);
                String tableName = ci.getTable().toLowerCase();
                if (!tableName.equals("")) {
                    map.put(tableName + "." + columnName, i);
                }
                i++;
            }
        }
        Integer res = map.get(name.toLowerCase());
        if (res == null) {
            // The specs in JDBC 4.0 specify that ResultSet.findColumn and
            // ResultSet.getXXX(String name) should use original column name rather than column alias (AS in the query)
            // However, in the past there was no consensus about it and different drivers implemented it differently
            // and for Connector/J the result might even differ from version to version, and is controlled by a special
            //  connection parameter.

            // To cope with this situation, we implement a fallback mechanism - we try the original column name as in
            // JDBC 4.0, if not found we try the label. The point is that user program continues to run rather than die
            // on exception.
            res = getLabelIndex(name);
        }
        if (res == null) {
            throw new SQLException("No such column :" + name);
        }
        return res;
    }

    private int getLabelIndex(String name) throws SQLException {
        if (labelMap == null) {
            labelMap = new HashMap<String, Integer>();
            int i=0;
            for(ColumnInformation ci : columnInfo) {
                String columnAlias = ci.getName().toLowerCase();
                labelMap.put(columnAlias, i);
                String tableName = ci.getTable().toLowerCase();
                if (!tableName.equals("")) {
                    labelMap.put(tableName + "." + columnAlias, i);
                }
                i++;
            }

        }
        Integer res = labelMap.get(name.toLowerCase());
        if (res == null) {
            throw new SQLException("No such column :" + name);
        }
        return res;
    }
}
