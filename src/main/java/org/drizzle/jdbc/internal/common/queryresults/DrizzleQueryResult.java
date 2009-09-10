/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.queryresults;

import org.drizzle.jdbc.internal.common.ColumnInformation;
import org.drizzle.jdbc.internal.common.ValueObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p/>
 * User: marcuse Date: Jan 23, 2009 Time: 8:15:55 PM
 */
public final class DrizzleQueryResult implements SelectQueryResult {
    private final List<ColumnInformation> columnInformation;
    private final List<List<ValueObject>> resultSet;
    private final Map<String, Integer> columnNameMap;
    private final short warningCount;
    private int rowPointer;

    public DrizzleQueryResult(final List<ColumnInformation> columnInformation,
                              final List<List<ValueObject>> valueObjects,
                              final short warningCount) {
        this.columnInformation = columnInformation;
        this.resultSet = valueObjects;
        this.warningCount = warningCount;
        columnNameMap = new HashMap<String, Integer>();
        rowPointer = -1;
        int i = 0;
        for (final ColumnInformation ci : columnInformation) {
            columnNameMap.put(ci.getName().toLowerCase(), i++);
        }
    }

    public boolean next() {
        rowPointer++;
        return rowPointer < resultSet.size();
    }

    public void close() {
        columnInformation.clear();
        resultSet.clear();
        columnNameMap.clear();
    }

    public short getWarnings() {
        return warningCount;
    }

    public String getMessage() {
        return null;
    }

    public List<ColumnInformation> getColumnInformation() {
        return columnInformation;
    }

    /**
     * gets the value at position i in the result set. i starts at zero!
     *
     * @param i index, starts at 0
     * @return
     */
    public ValueObject getValueObject(final int i) throws NoSuchColumnException {
        if (i < 0 || i > resultSet.get(rowPointer).size()) {
            throw new NoSuchColumnException("No such column: " + i);
        }
        return resultSet.get(rowPointer).get(i);
    }

    public ValueObject getValueObject(final String column) throws NoSuchColumnException {
        if (columnNameMap.get(column.toLowerCase()) == null) {
            throw new NoSuchColumnException("No such column: " + column);
        }
        return getValueObject(columnNameMap.get(column.toLowerCase()));
    }

    public int getRows() {
        return resultSet.size();
    }

    public int getColumnId(final String columnLabel) throws NoSuchColumnException {
        if (columnNameMap.get(columnLabel.toLowerCase()) == null) {
            throw new NoSuchColumnException("No such column: " + columnLabel);
        }
        return columnNameMap.get(columnLabel.toLowerCase());
    }

    public void moveRowPointerTo(final int i) {
        this.rowPointer = i;
    }

    public int getRowPointer() {
        return rowPointer;
    }


    public ResultSetType getResultSetType() {
        return ResultSetType.SELECT;
    }
}
