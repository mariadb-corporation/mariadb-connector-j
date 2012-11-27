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

package org.mariadb.jdbc.internal.common.queryresults;

import org.mariadb.jdbc.internal.common.ColumnInformation;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.ValueObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p/>
 * User: marcuse Date: Jan 23, 2009 Time: 8:15:55 PM
 */
public  class CachedSelectResult extends SelectQueryResult {

    private  List<List<ValueObject>> resultSet;
    protected short warningCount;
    private int rowPointer;

    public CachedSelectResult(List<ColumnInformation> ci, List<List<ValueObject>> result, short warningCount) {
        this.columnInformation = ci;
        this.resultSet = result;
        this.warningCount = warningCount;
        rowPointer = -1;
    }


    public static CachedSelectResult createCachedSelectResult(StreamingSelectResult streamingResult) throws IOException, QueryException {
         final List<List<ValueObject>> valueObjects = new ArrayList<List<ValueObject>>();

        while(streamingResult.next()){
           valueObjects.add(streamingResult.values);
        }
        CachedSelectResult qr = new CachedSelectResult(streamingResult.columnInformation, valueObjects, streamingResult.warningCount);
        streamingResult.close();
        return qr;
    }

    public boolean next() throws IOException, QueryException{
        rowPointer++;
        return rowPointer < resultSet.size();
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
        if (rowPointer < 0) {
            throw new NoSuchColumnException("Current position is before the first row");
        }
        if (rowPointer >= resultSet.size()) {
            throw new NoSuchColumnException("Current position is after the last row");
        }
        if (i < 0 || i > resultSet.get(rowPointer).size()) {
            throw new NoSuchColumnException("No such column: " + i);
        }
        return resultSet.get(rowPointer).get(i);
    }

    public int getRows() {
        return resultSet.size();
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
    public boolean isBeforeFirst() {
       if (resultSet.size() == 0)
           return false;
       return getRowPointer() == -1 ;
    }
    public boolean isAfterLast() {
       return rowPointer >= resultSet.size();
    }
}
