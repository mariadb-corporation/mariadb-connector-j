package org.mariadb.jdbc.internal.queryresults;
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

import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.util.ExceptionMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;


public class Results {

    private MariaDbStatement statement;
    private int fetchSize;
    private boolean batch;
    private int expectedSize;
    private CmdInformation cmdInformation;
    private Deque<SelectResultSet> executionResults;
    private SelectResultSet resultSet;
    private SelectResultSet callableResultSet;
    private boolean binaryFormat;
    private int resultSetScrollType;
    private int maxFieldSize;
    private int autoIncrement;

    /**
     * Single Text query.
     *
     * /! use internally, because autoincrement value is not right for multi-queries !/
     *
     */
    public Results() {
        this.statement = null;
        this.fetchSize = 0;
        this.maxFieldSize = 0;
        this.batch = false;
        this.expectedSize = 1;
        this.cmdInformation = null;
        this.binaryFormat = false;
        this.resultSetScrollType = ResultSet.TYPE_FORWARD_ONLY;
        this.autoIncrement = 1;
    }

    /**
     * Constructor for specific statement.
     * @param statement     current Statement.
     * @param autoIncrement connection auto-increment
     */
    public Results(MariaDbStatement statement, int autoIncrement) {
        this.statement = statement;
        this.fetchSize = 0;
        this.maxFieldSize = 0;
        this.batch = false;
        this.expectedSize = 1;
        this.cmdInformation = null;
        this.binaryFormat = false;
        this.resultSetScrollType = ResultSet.TYPE_FORWARD_ONLY;
        this.autoIncrement = autoIncrement;
    }

    /**
     * Default constructor.
     *
     * @param statement             current statement
     * @param fetchSize             fetch size
     * @param batch                 select result possible
     * @param expectedSize          expected size
     * @param binaryFormat          use binary protocol
     * @param resultSetScrollType   one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                              <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param autoIncrement         Connection auto-increment value
     */
    public Results(MariaDbStatement statement, int fetchSize, boolean batch, int expectedSize, boolean binaryFormat, int resultSetScrollType,
                   int autoIncrement) {
        this.statement = statement;
        this.fetchSize = fetchSize;
        this.batch = batch;
        this.maxFieldSize = statement.getMaxFieldSize();
        this.expectedSize = expectedSize;
        this.cmdInformation = null;
        this.binaryFormat = binaryFormat;
        this.resultSetScrollType = resultSetScrollType;
        this.autoIncrement = autoIncrement;
    }

    /**
     * Reset.
     *
     * @param fetchSize             fetch size
     * @param batch                 select result possible
     * @param expectedSize          expected size
     * @param binaryFormat          use binary protocol
     * @param resultSetScrollType   one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                              <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     */
    public void reset(int fetchSize, boolean batch, int expectedSize, boolean binaryFormat, int resultSetScrollType) {
        this.fetchSize = fetchSize;
        this.batch = batch;
        this.maxFieldSize = statement.getMaxFieldSize();
        this.expectedSize = expectedSize;
        this.cmdInformation = null;
        this.binaryFormat = binaryFormat;
        this.resultSetScrollType = resultSetScrollType;
    }

    /**
     * Add execution statistics.
     *
     * @param updateCount         number of updated rows
     * @param insertId            primary key
     * @param moreResultAvailable is there additional packet
     */
    public void addStats(long updateCount, long insertId, boolean moreResultAvailable) {
        if (cmdInformation == null) {
            if (batch) {
                cmdInformation = new CmdInformationBatch(expectedSize, autoIncrement);
            } else if (moreResultAvailable) {
                cmdInformation = new CmdInformationMultiple(expectedSize, autoIncrement);
            } else {
                cmdInformation = new CmdInformationSingle(insertId, updateCount, autoIncrement);
                return;
            }
        }
        cmdInformation.addSuccessStat(updateCount, insertId);
    }

    /**
     * Indicate that result is an Error, to set appropriate results.
     * @param moreResultAvailable indicate if other results (ResultSet or updateCount) are available.
     */
    public void addStatsError(boolean moreResultAvailable) {
        if (cmdInformation == null) {
            if (batch) {
                cmdInformation = new CmdInformationBatch(expectedSize, autoIncrement);
            } else if (moreResultAvailable) {
                cmdInformation = new CmdInformationMultiple(expectedSize, autoIncrement);
            } else {
                cmdInformation = new CmdInformationSingle(0, Statement.EXECUTE_FAILED, autoIncrement);
                return;
            }
        }
        cmdInformation.addErrorStat();
    }

    public int getCurrentStatNumber() {
        return (cmdInformation == null) ? 0 : cmdInformation.getCurrentStatNumber();
    }

    /**
     * Add resultSet to results.
     *
     * @param resultSet new resultSet.
     * @param moreResultAvailable indicate if other results (ResultSet or updateCount) are available.
     */
    public void addResultSet(SelectResultSet resultSet, boolean moreResultAvailable) {
        if (resultSet.isCallableResult()) {
            callableResultSet = resultSet;
            return;
        }
        if (executionResults == null) executionResults = new ArrayDeque<>();
        executionResults.add(resultSet);
        if (cmdInformation == null) {
            if (batch) {
                cmdInformation = new CmdInformationBatch(expectedSize, autoIncrement);
            } else if (moreResultAvailable) {
                cmdInformation = new CmdInformationMultiple(expectedSize, autoIncrement);
            } else {
                cmdInformation = new CmdInformationSingle(0, -1, autoIncrement);
                return;
            }
        }
        cmdInformation.addResultSetStat();
    }

    public CmdInformation getCmdInformation() {
        return cmdInformation;
    }

    /**
     * Indicate that command / batch is finished, so set current resultSet if needed.
     * @return current results
     */
    public Results commandEnd() {
        if (cmdInformation != null
                && executionResults != null
                && !cmdInformation.isCurrentUpdateCount()) {
            resultSet = executionResults.poll();
        } else {
            resultSet = null;
        }
        return this;
    }

    public SelectResultSet getResultSet() {
        return resultSet;
    }

    public SelectResultSet getCallableResultSet() {
        return callableResultSet;
    }

    /**
     * Load fully current results.
     *
     * <i>Lock must be set before using this method</i>
     *
     * @param skip      must result be available afterwhile
     * @param protocol  current protocol
     * @throws SQLException if any connection error occur
     */
    public void loadFully(boolean skip, Protocol protocol) throws SQLException {
        if (fetchSize != 0) {
            fetchSize = 0;
            if (resultSet != null) {
                if (skip) {
                    resultSet.close();
                } else {
                    resultSet.fetchRemaining();
                }
            } else {
                SelectResultSet firstResult = executionResults.peekFirst();
                if (firstResult != null ) {
                    if (skip) {
                        firstResult.close();
                    } else {
                        firstResult.fetchRemaining();
                    }
                }
            }
        }

        if (protocol.hasMoreResults()) protocol.getResult(this);
    }

    /**
     * Position to next resultSet.
     *
     * @param current   one of the following <code>Statement</code> constants indicating what should happen to current
     *                  <code>ResultSet</code> objects obtained using the method <code>getResultSet</code>:
     *                  <code>Statement.CLOSE_CURRENT_RESULT</code>, <code>Statement.KEEP_CURRENT_RESULT</code>,
     *                  or <code>Statement.CLOSE_ALL_RESULTS</code>
     * @param protocol  current protocol
     * @return true if other resultSet exists.
     * @throws SQLException if any connection error occur.
     */
    public boolean getMoreResults(final int current, Protocol protocol) throws SQLException {
        if (fetchSize != 0) {
            if (resultSet != null) {

                protocol.getLock().lock();
                try {
                    //load current resultSet
                    if (current == Statement.CLOSE_CURRENT_RESULT && resultSet != null) {
                        resultSet.close();
                    } else {
                        resultSet.fetchRemaining();
                    }

                    //load next data if there is
                    if (protocol.hasMoreResults()) protocol.getResult(this);

                } catch (SQLException e) {
                    ExceptionMapper.throwException(e, null, statement);
                } finally {
                    protocol.getLock().unlock();
                }

            }
        }

        if (cmdInformation.moreResults() && !batch) {

            if (current == Statement.CLOSE_CURRENT_RESULT && resultSet != null) resultSet.close();
            resultSet = null;
            return true;

        } else {

            if (current == Statement.CLOSE_CURRENT_RESULT && resultSet != null) resultSet.close();
            if (executionResults != null) resultSet = executionResults.poll();
            return resultSet != null;

        }

    }

    public int getFetchSize() {
        return fetchSize;
    }

    public MariaDbStatement getStatement() {
        return statement;
    }

    public boolean isBatch() {
        return batch;
    }

    protected void setCmdInformation(CmdInformation cmdInformation) {
        this.cmdInformation = cmdInformation;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public boolean isBinaryFormat() {
        return binaryFormat;
    }

    public void removeFetchSize() {
        fetchSize = 0;
    }

    public int getResultSetScrollType() {
        return resultSetScrollType;
    }

    /**
     * Send a resultSet that contain auto generated keys.
     * 2 differences :
     * <ol>
     *     <li>Batch will list all insert ids.</li>
     *     <li>in case of multi-query is set, resultSet will be per query. </li>
     * </ol>
     *
     * example "INSERT INTO myTable values ('a'),('b');INSERT INTO myTable values ('c'),('d'),('e')"
     * will have a resultSet of 2 values, and when Statement.getMoreResults() will be called,
     * a Statement.getGeneratedKeys will return a resultset with 3 ids.
     *
     * @param protocol current protocol
     * @return a ResultSet containing generated ids.
     */
    public ResultSet getGeneratedKeys(Protocol protocol) {
        if (cmdInformation != null) {
            if (batch) return cmdInformation.getBatchGeneratedKeys(protocol);
            return cmdInformation.getGeneratedKeys(protocol);
        }
        return SelectResultSet.createEmptyResultSet();
    }

    public void close() {
        statement = null;
        fetchSize = 0;
    }

    public int getMaxFieldSize() {
        return maxFieldSize;
    }
}
