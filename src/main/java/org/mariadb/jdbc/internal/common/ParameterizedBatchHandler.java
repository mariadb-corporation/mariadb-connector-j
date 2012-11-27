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

package org.mariadb.jdbc.internal.common;

import org.mariadb.jdbc.internal.common.query.ParameterizedQuery;

/**
 * Interface that defines a parameterized batch handler. Implement this interface and set it as a parameterized batch
 * handler on the connection like this: <code> if(connection.isWrapperFor(MySQLConnection.class)) { MySQLConnection
 * dc = connection.unwrap(MySQLConnection.class); dc.setBatchQueryHandler(VerrrryFastBatchHandler.class); } Note:
 * implementations currently need a default no-args constructor. </code>
 */
public interface ParameterizedBatchHandler {
    /**
     * called when a set of parameters are added to a batch.
     *
     * @param query the parameterized query.
     */
    void addToBatch(ParameterizedQuery query);

    /**
     * execute the batch using protocol. Return an array of update counts or -2 (Statement.SUCCESS_NO_INFO) if the
     * update count is unknown.
     *
     * @return a list of update counts
     * @throws QueryException if something goes wrong executing the query.
     */
    int[] executeBatch() throws QueryException;
}