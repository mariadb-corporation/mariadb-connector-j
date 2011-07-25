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

import org.skysql.jdbc.internal.common.ValueObject;

/**
 * User: marcuse Date: Mar 9, 2009 Time: 8:42:45 PM
 */
public interface SelectQueryResult extends QueryResult {


    /**
     * gets the value object at position index, starts at 0
     *
     * @param index the position, starts at 0
     * @return the value object at position index
     * @throws NoSuchColumnException if the column does not exist
     */
    ValueObject getValueObject(int index) throws NoSuchColumnException;

    /**
     * gets the value object in column named columnName
     *
     * @param columnName the name of the column
     * @return a value object
     * @throws NoSuchColumnException if the column does not exist
     */
    ValueObject getValueObject(String columnName) throws NoSuchColumnException;

    /**
     * get the id of the column named columnLabel
     *
     * @param columnLabel the label of the column
     * @return the index, starts at 0
     * @throws NoSuchColumnException if the column does not exist
     */
    int getColumnId(String columnLabel) throws NoSuchColumnException;

    /**
     * moves the row pointer to position i
     *
     * @param i the position
     */
    void moveRowPointerTo(int i);

    /**
     * gets the current row number
     *
     * @return the current row number
     */
    int getRowPointer();

    /**
     * move pointer forward
     *
     * @return true if there is another row
     */
    boolean next();
}
