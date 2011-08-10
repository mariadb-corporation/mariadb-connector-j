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

package org.skysql.jdbc.internal.common.queryresults;

import org.skysql.jdbc.internal.common.ColumnInformation;
import org.skysql.jdbc.internal.common.QueryException;
import org.skysql.jdbc.internal.common.ValueObject;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * User: marcuse Date: Mar 9, 2009 Time: 8:42:45 PM
 */
public abstract class SelectQueryResult implements QueryResult {

    List<ColumnInformation> columnInformation;
    short warningCount;

    public List<ColumnInformation> getColumnInformation() {
        return columnInformation;
    }

    public short getWarnings() {
        return warningCount;
    }

    public String getMessage() {
        return null;
    }

    public ResultSetType getResultSetType() {
        return ResultSetType.SELECT;
    }

     /**
     * moves the row pointer to position i
     *
     * @param i the position
     */
    public  void moveRowPointerTo(int i) throws SQLException{
        throw new SQLFeatureNotSupportedException("scrolling result set not supported");
    }

    /**
     * gets the current row number
     *
     * @return the current row number
     */
    public int getRowPointer() throws SQLException{
        throw new SQLFeatureNotSupportedException("scrolling result set not supported");
    }

    /**
     * move pointer forward
     *
     * @return true if there is another row
     */
    /**
     * gets the value object at position index, starts at 0
     *
     * @param index the position, starts at 0
     * @return the value object at position index
     * @throws NoSuchColumnException if the column does not exist
     */
    public abstract ValueObject getValueObject(int index) throws NoSuchColumnException;


    public abstract boolean next() throws IOException, QueryException;
    public abstract boolean isBeforeFirst();
    public abstract boolean isAfterLast();
}
