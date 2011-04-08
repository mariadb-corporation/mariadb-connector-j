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

package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.SQLExceptionMapper;
import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;
import org.drizzle.jdbc.internal.common.queryresults.ModifyQueryResult;
import org.drizzle.jdbc.internal.common.queryresults.QueryResult;

import java.util.LinkedList;
import java.util.List;

public final class DefaultParameterizedBatchHandler implements ParameterizedBatchHandler {
    private final List<ParameterizedQuery> queries = new LinkedList<ParameterizedQuery>();
    private final Protocol protocol;

    public DefaultParameterizedBatchHandler(final Protocol protocol) {
        this.protocol = protocol;
    }

    public void addToBatch(final ParameterizedQuery query) {
        queries.add(query);
    }

    public int[] executeBatch() throws QueryException {
        final int[] retArray = new int[queries.size()];
        int i = 0;
        for (final ParameterizedQuery query : queries) {
            final QueryResult qr = protocol.executeQuery(query);
            if (qr instanceof ModifyQueryResult) {
                retArray[i++] = (int) ((ModifyQueryResult) qr).getUpdateCount();
            } else {
                throw new QueryException("One of the queries in the batch returned a result set",
                        (short) -1,
                        SQLExceptionMapper.SQLStates.UNDEFINED_SQLSTATE.getSqlState());
            }
        }
        queries.clear();
        return retArray;
    }
}
