package org.mariadb.jdbc.internal.common.queryresults;

import org.mariadb.jdbc.internal.mysql.ColumnInformation;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class ColumnNameMap {
    Map<String, Integer> map;
    Map<String, Integer> labelMap;
    ColumnInformation[] columnInfo;

    public ColumnNameMap(ColumnInformation[] columnInformations) {
        this.columnInfo = columnInformations;
    }

    /**
     * Get column index by name.
     * @param name column name
     * @return index.
     * @throws SQLException if no column info exists, or column is unknown
     */
    public int getIndex(String name) throws SQLException {
        if (columnInfo == null) {
            throw new SQLException("No such column :" + name);
        }
        // The specs in JDBC 4.0 specify that ResultSet.findColumn and
        // ResultSet.getXXX(String name) should use column alias (AS in the query). If label is not found, we use 
        // original table name.
        Integer res = getLabelIndex(name);


        if (res != null) {
            return res;
        }
        if (map == null) {
            map = new HashMap<String, Integer>();
            int counter = 0;
            for (ColumnInformation ci : columnInfo) {
                String columnName = ci.getOriginalName().toLowerCase();
                if (columnName.equals("")) {
                    // for name-less columns (there CAN be some), use their alias
                    columnName = ci.getName().toLowerCase();
                }
                map.put(columnName, counter);
                String tableName = ci.getTable().toLowerCase();
                if (!tableName.equals("")) {
                    map.put(tableName + "." + columnName, counter);
                }
                counter++;
            }
        }
        res = map.get(name.toLowerCase());

        if (res == null) {
            throw new SQLException("No such column :" + name);
        }
        return res;
    }

    private int getLabelIndex(String name) throws SQLException {
        if (labelMap == null) {
            labelMap = new HashMap<String, Integer>();
            int counter = 0;
            for (ColumnInformation ci : columnInfo) {
                String columnAlias = ci.getName().toLowerCase();
                if (!labelMap.containsKey(columnAlias)) {
                    labelMap.put(columnAlias, counter);
                }

                if (ci.getTable() != null) {
                    String tableName = ci.getTable().toLowerCase();
                    if (!tableName.equals("")) {
                        if (!labelMap.containsKey(tableName + "." + columnAlias)) {
                            labelMap.put(tableName + "." + columnAlias, counter);
                        }
                    }
                }
                counter++;
            }
        }
        Integer res = labelMap.get(name.toLowerCase());
        if (res == null) {
            throw new SQLException("No such column :" + name);
        }
        return res;
    }
}
