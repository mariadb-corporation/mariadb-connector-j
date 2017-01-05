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

import org.mariadb.jdbc.internal.packet.dao.parameters.NullParameter;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.util.dao.CloneableCallableStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MariaDbProcedureStatement extends AbstractCallableProcedureStatement implements CloneableCallableStatement {

    private MariaSelectResultSet outputResultSet = null;

    /**
     * Specific implementation of CallableStatement to handle function call, represent by call like
     * {?= call procedure-name[(arg1,arg2, ...)]}.
     *
     * @param query         query
     * @param connection    current connection
     * @param procedureName procedure name
     * @param database      database
     * @throws SQLException exception
     */
    public MariaDbProcedureStatement(String query, MariaDbConnection connection,
                                     String procedureName, String database) throws SQLException {
        super(connection, query, ResultSet.TYPE_FORWARD_ONLY);
        this.parameterMetadata = new CallableParameterMetaData(connection, database, procedureName, false);
        setParamsAccordingToSetArguments();
        setParametersVariables();
    }

    private void setParamsAccordingToSetArguments() throws SQLException {
        params = new ArrayList<>(this.parameterCount);
        for (int index = 0; index < this.parameterCount; index++) {
            params.add(new CallParameter());
        }
    }

    private void setInputOutputParameterMap() {
        if (outputParameterMapper == null) {
            outputParameterMapper = new int[params.size()];
            int currentOutputMapper = 1;

            for (int index = 0; index < params.size(); index++) {
                outputParameterMapper[index] = params.get(index).isOutput ? currentOutputMapper++ : -1;
            }
        }
    }

    protected MariaSelectResultSet getResult() throws SQLException {
        if (outputResultSet == null) {
            throw new SQLException("No output result.");
        }
        return outputResultSet;
    }

    /**
     * Clone statement.
     *
     * @return Clone statement.
     * @throws CloneNotSupportedException if any error occur.
     */
    public MariaDbProcedureStatement clone() throws CloneNotSupportedException {
        MariaDbProcedureStatement clone = (MariaDbProcedureStatement) super.clone();
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
    public int executeUpdate() throws SQLException {
        validAllParameters();
        connection.lock.lock();
        try {
            super.executeInternal(0);
            retrieveOutputResult();
            return getUpdateCount();
        } finally {
            connection.lock.unlock();
        }
    }

    private void retrieveOutputResult() throws SQLException {
        //resultSet will be just before last packet
        outputResultSet = results.getCallableResultSet();
        if (outputResultSet != null) {
            outputResultSet.next();
        }
    }

    protected void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {
        params.get(parameterIndex - 1).isInput = true;
        super.setParameter(parameterIndex, holder);
    }

    @Override
    public boolean execute() throws SQLException {
        connection.lock.lock();
        try {
            validAllParameters();
            super.executeInternal(0);
            retrieveOutputResult();
            return results != null && results.getResultSet() != null;
        } finally {
            connection.lock.unlock();
        }
    }


    /**
     * Valid that all parameters are set.
     *
     * @throws SQLException if set parameters is not right
     */
    private void validAllParameters() throws SQLException {

        setInputOutputParameterMap();
        //Set value for OUT parameters
        for (int index = 0; index < params.size(); index++) {
            if (!params.get(index).isInput) {
                super.setParameter(index + 1, new NullParameter());
            }
        }
        validParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (!hasInOutParameters) {
            return super.executeBatch();
        } else {
            throw new SQLException("executeBatch not permit for procedure with output parameter");
        }
    }

}
