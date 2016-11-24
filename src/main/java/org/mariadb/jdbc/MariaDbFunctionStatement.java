/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2016 MariaDB Corporation AB

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

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.util.dao.CloneableCallableStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MariaDbFunctionStatement extends AbstractCallableFunctionStatement implements CloneableCallableStatement {

    private MariaSelectResultSet outputResultSet = null;

    /**
     * Specific implementation of CallableStatement to handle function call, represent by call like
     * {?= call procedure-name[(arg1,arg2, ...)]}.
     *
     * @param connection    current connection
     * @param databaseName  database name
     * @param procedureName function name
     * @param arguments     function args
     * @throws SQLException exception
     */
    public MariaDbFunctionStatement(MariaDbConnection connection, String databaseName, String procedureName, String arguments) throws SQLException {
        super(connection, "SELECT " + procedureName + ((arguments == null) ? "()" : arguments),
                ResultSet.TYPE_FORWARD_ONLY);
        parameterMetadata = new CallableParameterMetaData(connection, databaseName, procedureName, true);
        super.initFunctionData(getParameterCount() + 1);
    }

    protected MariaSelectResultSet getResult() throws SQLException {
        if (outputResultSet == null) {
            throw new SQLException("No output result");
        }
        return outputResultSet;
    }

    /**
     * Clone statement.
     *
     * @return Clone statement.
     * @throws CloneNotSupportedException if any error occur.
     */
    public MariaDbFunctionStatement clone() throws CloneNotSupportedException {
        MariaDbFunctionStatement clone = (MariaDbFunctionStatement) super.clone();
        clone.outputResultSet = null;
        return clone;
    }

    /**
     * Executes the CALL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements
     * that return nothing
     * @throws SQLException if a database access error occurs; this method is called on a closed
     *                      <code>PreparedStatement</code> or the SQL statement returns a
     *                      <code>ResultSet</code> object
     */
    @Override
    public int executeUpdate() throws SQLException {
        connection.lock.lock();
        try {
            super.execute();
            retrieveOutputResult();
            if (executionResult != null && executionResult.getResultSet() == null) {
                return 0;
            }
            return getUpdateCount();
        } finally {
            connection.lock.unlock();
        }
    }


    private void retrieveOutputResult() throws SQLException {
        outputResultSet = executionResult.getResultSet();
        if (outputResultSet != null) {
            outputResultSet.next();
        }
    }


    protected void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {
        super.setParameter(parameterIndex - 1, holder);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        connection.lock.lock();
        try {
            super.execute();
            retrieveOutputResult();
            if (executionResult != null && executionResult.getResultSet() == null) {
                return executionResult.getResultSet();
            }
            return MariaSelectResultSet.EMPTY;
        } finally {
            connection.lock.unlock();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        connection.lock.lock();
        try {
            super.execute();
            retrieveOutputResult();
            return executionResult != null && executionResult.getResultSet() == null;
        } finally {
            connection.lock.unlock();
        }
    }

}
