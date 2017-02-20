/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye, Stephane Giron

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

package org.mariadb.jdbc;


import org.mariadb.jdbc.internal.queryresults.ResultsRewrite;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MariaDbPreparedStatementClient extends BasePreparedStatementClient implements PreparedStatement {

    /**
     * Constructor for Jdbc42 compatible PrepareStatement with client prepare (parsing).
     *
     * @param connection          connection
     * @param sql                 sql query
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants:
     *                            <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *                            <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws SQLException exception
     */
    public MariaDbPreparedStatementClient(MariaDbConnection connection, String sql, int resultSetScrollType) throws SQLException {
        super(connection, sql, resultSetScrollType);
    }

    /**
     * Execute batch, like executeBatch(), with returning results with long[].
     * For when row count may exceed Integer.MAX_VALUE.
     *
     * @return an array of update counts (one element for each command in the batch)
     * @throws SQLException if a database error occur.
     */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkClose();
        int size = parameterList.size();
        if (size == 0) return new long[0];

        lock.lock();
        try {

            executeInternalBatch(size);

            return results.getCmdInformation().getLargeUpdateCounts();

        } catch (SQLException sqle) {
            throw executeBatchExceptionEpilogue(sqle, results.getCmdInformation(), size);
        } finally {
            executeBatchEpilogue();
            lock.unlock();
        }
    }
}
