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

import org.mariadb.jdbc.internal.queryresults.CmdInformation;
import org.mariadb.jdbc.internal.queryresults.Results;
import org.mariadb.jdbc.internal.util.ExceptionMapper;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Add JDBC42 addition that are not compatible with java 7 jre.
 */
public class MariaDbStatement extends BaseStatement implements Statement {

    /**
     * Constructor for Jdbc42 Statement compatible implementation.
     *
     * @param connection          current connection
     * @param resultSetScrollType result set scroll type
     */
    public MariaDbStatement(MariaDbConnection connection, int resultSetScrollType) {
        super(connection, resultSetScrollType);
        this.results = new Results(this, connection.getAutoIncrementIncrement());
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
        int size;
        if (batchQueries == null || (size = batchQueries.size()) == 0) return new long[0];

        lock.lock();
        try {
            internalBatchExecution(size);
            return results.getCmdInformation().getLargeUpdateCounts();

        } catch (SQLException initialSqlEx) {
            throw executeLargeBatchExceptionEpilogue(initialSqlEx, results.getCmdInformation(), size);
        } finally {
            executeBatchEpilogue();
            lock.unlock();
        }
    }

    /**
     * Handle Exception for large batch update (return BatchUpdateException with long[].
     *
     * @param exception      initial exception
     * @param cmdInformation command return information (to indicate output that have been executed)
     * @param size           initial batch length
     * @return a BatchUpdateException
     */
    private BatchUpdateException executeLargeBatchExceptionEpilogue(SQLException exception, CmdInformation cmdInformation, int size) {
        exception = handleFailoverAndTimeout(exception);
        long[] ret;
        if (cmdInformation == null) {
            ret = new long[size];
            Arrays.fill(ret, Statement.EXECUTE_FAILED);
        } else ret = cmdInformation.getLargeUpdateCounts();

        exception = ExceptionMapper.getException(exception, connection, this, getQueryTimeout() != 0);
        logger.error("error executing query", exception);

        return new BatchUpdateException(exception.getMessage(), exception.getSQLState(), exception.getErrorCode(), ret, exception);
    }

    /**
     * Executes the given SQL statement, which may be an INSERT, UPDATE, or DELETE statement or an SQL statement that returns nothing,
     * such as an SQL DDL statement.
     * This method should be used when the returned row count may exceed Integer.MAX_VALUE.
     *
     * @param sql sql command
     * @return update counts
     * @throws SQLException if any error occur during execution
     */
    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        if (executeInternal(sql, fetchSize)) {
            return 0;
        }
        return getLargeUpdateCount();
    }

    /**
     * Identical to executeLargeUpdate(String sql), with a flag that indicate that autoGeneratedKeys (primary key fields with "auto_increment")
     * generated id's must be retrieved.
     *
     * Those id's will be available using getGeneratedKeys() method.
     *
     * @param sql               sql command
     * @param autoGeneratedKeys a flag indicating whether auto-generated keys should be made available for retrieval;
     *                          one of the following constants:
     *                          Statement.RETURN_GENERATED_KEYS
     *                          Statement.NO_GENERATED_KEYS
     * @return update counts
     * @throws SQLException if any error occur during execution
     */
    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        //driver always get generated keys.
        return executeLargeUpdate(sql);
    }

    /**
     * Identical to executeLargeUpdate(String sql, int autoGeneratedKeys) with autoGeneratedKeys = Statement.RETURN_GENERATED_KEYS set.
     *
     * @param sql               sql command
     * @param columnIndexes     column Indexes
     * @return update counts
     * @throws SQLException if any error occur during execution
     */
    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        //driver always get generated keys. no need for columnIndexes indication
        return executeLargeUpdate(sql);
    }

    /**
     * Identical to executeLargeUpdate(String sql, int autoGeneratedKeys) with autoGeneratedKeys = Statement.RETURN_GENERATED_KEYS set.
     *
     * @param sql               sql command
     * @param columnNames       columns names
     * @return update counts
     * @throws SQLException if any error occur during execution
     */
    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        //driver always get generated keys. no need for columnNames indication
        return executeLargeUpdate(sql);
    }

    /**
     * Retrieves the maximum number of rows that a ResultSet object produced by this Statement object can contain.
     * If this limit is exceeded, the excess rows are silently dropped.
     * @return the current maximum number of rows for a ResultSet object produced by this Statement object; zero means there is no limit
     */
    @Override
    public long getLargeMaxRows() {
        return maxRows;
    }

    /**
     * Sets the limit for the maximum number of rows that any ResultSet object generated by this Statement object can contain to the given number.
     * If the limit is exceeded, the excess rows are silently dropped.
     *
     * @param max the new max rows limit; zero means there is no limit
     * @throws SQLException  if the condition max >= 0 is not satisfied
     */
    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max rows cannot be negative : setLargeMaxRows value is " + max);
        }
        maxRows = max;
    }

    /**
     * Retrieves the current result as an update count; if the result is a ResultSet object or there are no more results, -1 is returned.
     *
     * @return last update count
     */
    @Override
    public long getLargeUpdateCount() {
        if (results.getCmdInformation() != null) {
            return results.getCmdInformation().getLargeUpdateCount();
        }
        return -1;
    }

}
