/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
