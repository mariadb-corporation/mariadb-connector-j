package org.mariadb.jdbc.internal.queryresults;
/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CachedSelectResult extends SelectQueryResult {

    protected short warningCount;
    private List<ValueObject[]> resultSet;
    private int rowPointer;


    /**
     * Initialisation.
     * @param ci column informations
     * @param result first valueObject
     * @param warningCount warning count
     */
    public CachedSelectResult(ColumnInformation[] ci, List<ValueObject[]> result, short warningCount) {
        this.columnInformation = ci;
        this.resultSet = result;
        this.warningCount = warningCount;
        rowPointer = -1;
    }

    /**
     * When using rewrite statement, there can be many insert/update command send to database, according to max_allowed_packet size.
     * the result will be aggregate with this method to give only one result stream to client.
     * @param other other AbstractQueryResult.
     */
    public void addResult(AbstractQueryResult other) {
        if (other.prepareResult != null) {
            prepareResult = other.prepareResult;
        }
        isClosed = other.isClosed();
    }

    /**
     * Will retrieve all "next" stream, and cache them.
     * @param streamingResult streamingResult to populate
     * @return ResultSet with all cached datas.
     * @throws IOException if any error occur during retreiving "next"s packets
     * @throws QueryException if receiving an database error stream
     */
    public static CachedSelectResult createCachedSelectResult(StreamingSelectResult streamingResult) throws IOException, QueryException {
        final List<ValueObject[]> valueObjects = new ArrayList<>();

        while (streamingResult.next()) {
            valueObjects.add(streamingResult.values);
        }
        CachedSelectResult qr = new CachedSelectResult(streamingResult.columnInformation, valueObjects, streamingResult.warningCount);
        streamingResult.close();
        return qr;
    }

    public boolean next() throws IOException, QueryException {
        rowPointer++;
        return rowPointer < resultSet.size();
    }


    public short getWarnings() {
        return warningCount;
    }

    public String getMessage() {
        return null;
    }

    public ColumnInformation[] getColumnInformation() {
        return columnInformation;
    }

    /**
     * gets the value at position i in the result set. i starts at zero!
     *
     * @param position index, starts at 0
     * @return the value
     */
    public ValueObject getValueObject(int position) throws NoSuchColumnException {
        if (this.rowPointer < 0) {
            throw new NoSuchColumnException("Current position is before the first row");
        }
        if (this.rowPointer >= resultSet.size()) {
            throw new NoSuchColumnException("Current position is after the last row");
        }
        ValueObject[] row = resultSet.get(this.rowPointer);
        if (position < 0 || position >= row.length) {
            throw new NoSuchColumnException("No such column: " + position);
        }
        return row[position];
    }

    public int getRows() {
        return resultSet.size();
    }

    public void moveRowPointerTo(final int pointerPosition) {
        this.rowPointer = pointerPosition;
    }

    public int getRowPointer() {
        return rowPointer;
    }


    public ResultSetType getResultSetType() {
        return ResultSetType.SELECT;
    }

    /**
     * Is pointer before first row.
     * @return true if pointer is before first row.
     */
    public boolean isBeforeFirst() {
        if (resultSet.size() == 0) {
            return false;
        }
        return getRowPointer() == -1;
    }

    /**
     * Is pointer after last row.
     * @return true if pointer is after last row
     */
    public boolean isAfterLast() {
        if (resultSet.size() == 0) {
            return false;
        }
        return rowPointer >= resultSet.size();
    }
}
