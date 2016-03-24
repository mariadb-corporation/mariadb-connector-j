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

package org.mariadb.jdbc.internal.queryresults.resultset;

import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbResultSetMetaData;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.packet.read.Packet;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.ColumnNameMap;
import org.mariadb.jdbc.internal.queryresults.resultset.value.GeneratedKeyValueObject;
import org.mariadb.jdbc.internal.queryresults.resultset.value.MariaDbValueObject;
import org.mariadb.jdbc.internal.queryresults.resultset.value.ValueObject;
import org.mariadb.jdbc.internal.util.ExceptionCode;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class MariaSelectResultSet implements ResultSet {
    public static final MariaSelectResultSet EMPTY = createEmptyResultSet();

    private Protocol protocol;
    private ReadPacketFetcher packetFetcher;
    private Statement statement;
    private RowPacket rowPacket;
    private ColumnInformation[] columnsInformation;

    private boolean isEof;
    private boolean binaryProtocol;
    private int dataFetchTime;
    private boolean streaming;
    private int columnInformationLength;
    private List<ValueObject[]> resultSet;
    private int fetchSize;
    private int resultSetScrollType;
    private int rowPointer;
    private ColumnNameMap columnNameMap;
    private Calendar cal;
    private boolean lastGetWasNull;
    private int dataTypeMappingFlags;
    private Options options;
    private boolean returnTableAlias;
    private boolean isClosed;
    public boolean callableResult = false;

    /**
     * Create Streaming resultset.
     *
     * @param columnInformation column information
     * @param statement statement
     * @param protocol current protocol
     * @param fetcher stream fetcher
     * @param binaryProtocol is binary protocol ?
     * @param resultSetScrollType  one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param fetchSize current fetch size
     */
    public MariaSelectResultSet(ColumnInformation[] columnInformation, Statement statement, Protocol protocol,
                                ReadPacketFetcher fetcher, boolean binaryProtocol,
                                int resultSetScrollType, int fetchSize) {

        this.statement = statement;
        this.isClosed = false;
        this.protocol = protocol;
        if (protocol != null) {
            this.options = protocol.getOptions();
            this.cal = protocol.getCalendar();
            this.dataTypeMappingFlags = protocol.getDataTypeMappingFlags();
            this.returnTableAlias = this.options.useOldAliasMetadataBehavior;
        } else {
            this.options = null;
            this.cal = null;
            this.dataTypeMappingFlags = 3;
            this.returnTableAlias = false;
        }
        this.columnsInformation = columnInformation;
        this.columnNameMap = new ColumnNameMap(columnsInformation);
        this.statement = statement;


        this.columnInformationLength = columnInformation.length;
        this.packetFetcher = fetcher;
        this.isEof = false;
        this.binaryProtocol = binaryProtocol;
        this.fetchSize = fetchSize;
        this.resultSetScrollType = resultSetScrollType;
        this.resultSet = new ArrayList<>();
        this.dataFetchTime = 0;
        this.rowPointer = -1;
    }

    /**
     * Create filled resultset.
     *
     * @param columnInformation column information
     * @param resultSet resultset
     * @param protocol current protocol
     * @param resultSetScrollType  one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     */
    public MariaSelectResultSet(ColumnInformation[] columnInformation, List<ValueObject[]> resultSet, Protocol protocol,
                                int resultSetScrollType) {
        this.statement = null;
        this.isClosed = false;
        this.protocol = protocol;
        if (protocol != null) {
            this.options = protocol.getOptions();
            this.cal = protocol.getCalendar();
            this.dataTypeMappingFlags = protocol.getDataTypeMappingFlags();
            this.returnTableAlias = this.options.useOldAliasMetadataBehavior;
        } else {
            this.options = null;
            this.cal = null;
            this.dataTypeMappingFlags = 3;
            this.returnTableAlias = false;
        }
        this.columnsInformation = columnInformation;
        this.columnNameMap = new ColumnNameMap(columnsInformation);
        this.columnInformationLength = columnInformation.length;
        this.isEof = false;
        this.binaryProtocol = false;
        this.fetchSize = 1;
        this.resultSetScrollType = resultSetScrollType;
        this.resultSet = resultSet;
        this.dataFetchTime = 0;
        this.rowPointer = -1;
    }

    /**
     * Create a result set from given data. Useful for creating "fake" resultsets for DatabaseMetaData, (one example is
     * MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param data - each element of this array represents a complete row in the ResultSet. Each value is given in its string representation, as in
     * MySQL text protocol, except boolean (BIT(1)) values that are represented as "1" or "0" strings
     * @param protocol protocol
     * @param findColumnReturnsOne - special parameter, used only in generated key result sets
     * @return resultset
     */
    public static ResultSet createGeneratedData(long[] data, Protocol protocol, boolean findColumnReturnsOne) {
        ColumnInformation[] columns = new ColumnInformation[1];
        columns[0] = ColumnInformation.create("insert_id", MariaDbType.BIGINT);

        List<ValueObject[]> rows = new ArrayList<>();
        for (Long rowData : data) {
            if (rowData != 0) {
                ValueObject[] row = new ValueObject[1];
                row[0] = new GeneratedKeyValueObject(rowData);
                rows.add(row);
            }
        }
        if (findColumnReturnsOne) {
            return new MariaSelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE) {
                @Override
                public int findColumn(String name) {
                    return 1;
                }
            };
        }
        return new MariaSelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
    }


    /**
     * Create a result set from given data. Useful for creating "fake" resultsets for DatabaseMetaData, (one example is
     * MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param columnNames - string array of column names
     * @param columnTypes - column types
     * @param data - each element of this array represents a complete row in the ResultSet. Each value is given in its string representation, as in
     * MySQL text protocol, except boolean (BIT(1)) values that are represented as "1" or "0" strings
     * @param protocol protocol
     * @return resultset
     */
    public static ResultSet createResultSet(String[] columnNames, MariaDbType[] columnTypes, String[][] data,
                                            Protocol protocol) {
        int columnNameLength = columnNames.length;
        ColumnInformation[] columns = new ColumnInformation[columnNameLength];

        for (int i = 0; i < columnNameLength; i++) {
            columns[i] = ColumnInformation.create(columnNames[i], columnTypes[i]);
        }

        final byte[] boolTrue = {1};
        final byte[] boolFalse = {0};
        List<ValueObject[]> rows = new ArrayList<>();
        for (String[] rowData : data) {
            ValueObject[] row = new ValueObject[columnNameLength];

            if (rowData.length != columnNameLength) {
                throw new RuntimeException("Number of elements in the row != number of columns :" + rowData.length + " vs " + columnNameLength);
            }
            for (int i = 0; i < columnNameLength; i++) {
                byte[] bytes;
                if (rowData[i] == null) {
                    bytes = null;
                } else if (columnTypes[i] == MariaDbType.BIT) {
                    bytes = rowData[i].equals("0") ? boolFalse : boolTrue;
                } else {
                    try {
                        bytes = rowData[i].getBytes("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        //never append, UTF-8 is known
                        bytes = new byte[0];
                    }
                }
                row[i] = new MariaDbValueObject(bytes, columns[i], protocol.getOptions());
            }
            rows.add(row);
        }

        return new MariaSelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
    }

    private static MariaSelectResultSet createEmptyResultSet() {
        return new MariaSelectResultSet(new ColumnInformation[0], new ArrayList<ValueObject[]>(), null,
                TYPE_SCROLL_SENSITIVE);
    }

    /**
     * Initialize and fetch first value.
     * @throws IOException exception
     * @throws QueryException exception
     */
    public void initFetch() throws IOException, QueryException {
        if (binaryProtocol) {
            rowPacket = new BinaryRowPacket(columnsInformation, protocol.getOptions(), columnInformationLength);
        } else {
            rowPacket = new TextRowPacket(columnsInformation, protocol.getOptions(), columnInformationLength);
        }
        if (fetchSize == 0 || resultSetScrollType != TYPE_FORWARD_ONLY) {
            fetchAllResults();
            streaming = false;
        } else {
            protocol.setActiveStreamingResult(this);
            nextStreamingValue();
            streaming = true;
        }
    }

    public boolean isBinaryProtocol() {
        return binaryProtocol;
    }

    private void fetchAllResults() throws IOException, QueryException {
        final List<ValueObject[]> valueObjects = new ArrayList<>();
        while (readNextValue(valueObjects)) {
            //fetch all results
        }
        dataFetchTime++;
        resultSet = valueObjects;
    }

    /**
     * When protocol has a current Streaming result (this) fetch all to permit another query is executing.
     *
     * @throws SQLException if any error occur
     */
    public void fetchAllStreaming() throws SQLException {
        try {
            try {
                Protocol protocolTmp = this.protocol;
                while (readNextValue(resultSet)) {
                    //fetch all results
                }

                //retrieve other results if needed
                if (protocolTmp.hasMoreResults()) {
                    if (this.statement != null) {
                        this.statement.getMoreResults();
                    }
                }
            } catch (IOException ioexception) {
                throw new QueryException("Could not close resultset : " + ioexception.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), ioexception);
            }
        } catch (QueryException queryException) {
            ExceptionMapper.throwException(queryException, null, this.getStatement());
        }
        dataFetchTime++;
        streaming = false;
    }



    private void nextStreamingValue() throws IOException, QueryException {

        final List<ValueObject[]> valueObjects = new ArrayList<>(fetchSize);
        //fetch maximum fetchSize results
        int fetchSizeTmp = fetchSize;
        while (fetchSizeTmp > 0 && readNextValue(valueObjects)) {
            fetchSizeTmp--;
        }
        dataFetchTime++;
        resultSet = valueObjects;
    }

    /**
     * Read next value.
     * @param values values
     * @return true if have a new value
     * @throws IOException exception
     * @throws QueryException exception
     */
    public boolean readNextValue(List<ValueObject[]> values) throws IOException, QueryException {
        Buffer buffer = packetFetcher.getPacket();

        //is error Packet
        if (buffer.getByteAt(0) == Packet.ERROR) {
            protocol.setActiveStreamingResult(null);
            ErrorPacket errorPacket = new ErrorPacket(buffer);
            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        //is EOF stream
        if ((buffer.getByteAt(0) == Packet.EOF && buffer.limit < 9)) {
            if (protocol.getActiveStreamingResult() == this) {
                protocol.setActiveStreamingResult(null);
            }
            protocol.setHasWarnings(((buffer.buf[1] & 0xff) + ((buffer.buf[2] & 0xff) << 8)) > 0);
            protocol.setMoreResults((((buffer.buf[3] & 0xff) + ((buffer.buf[4] & 0xff) << 8)) & ServerStatus.MORE_RESULTS_EXISTS) != 0, binaryProtocol);
            protocol = null;
            packetFetcher = null;
            isEof = true;
            return false;
        }
        values.add(rowPacket.getRow(packetFetcher, buffer));
        return true;
    }

    /**
     * Close resultset.
     */
    public void close() throws SQLException {
        isClosed = true;
        if (protocol != null && protocol.getActiveStreamingResult() == this) {
            ReentrantLock lock = protocol.getLock();
            lock.lock();
            try {
                try {
                    while (!isEof) {
                        //fetch all results
                        Buffer buffer = packetFetcher.getReusableBuffer();

                        //is error Packet
                        if (buffer.getByteAt(0) == Packet.ERROR) {
                            protocol.setActiveStreamingResult(null);
                            ErrorPacket errorPacket = new ErrorPacket(buffer);
                            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
                        }

                        //is EOF stream
                        if ((buffer.getByteAt(0) == Packet.EOF && buffer.limit < 9)) {
                            final EndOfFilePacket endOfFilePacket = new EndOfFilePacket(buffer);
                            if (protocol.getActiveStreamingResult() == this) {
                                protocol.setActiveStreamingResult(null);
                            }
                            protocol.setHasWarnings(endOfFilePacket.getWarningCount() > 0);
                            protocol.setMoreResults((endOfFilePacket.getStatusFlags() & ServerStatus.MORE_RESULTS_EXISTS) != 0, binaryProtocol);
                            protocol = null;
                            packetFetcher = null;
                            isEof = true;
                        }
                    }
                } catch (IOException ioexception) {
                    throw new QueryException("Could not close resultset : " + ioexception.getMessage(), -1,
                            ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), ioexception);
                }
            } catch (QueryException queryException) {
                ExceptionMapper.throwException(queryException, null, this.getStatement());
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkClose();
        return internalNext();
    }

    private boolean internalNext() throws SQLException {
        if (rowPointer < resultSet.size() - 1) {
            rowPointer++;
            return true;
        } else {
            if (streaming) {
                if (isEof) {
                    return isEof;
                } else {
                    try {
                        nextStreamingValue();
                    } catch (IOException ioe) {
                        throw new SQLException(ioe);
                    } catch (QueryException queryException) {
                        throw new SQLException(queryException);
                    }
                    rowPointer = 0;
                    return resultSet.size() > 0;
                }
            } else {
                rowPointer = resultSet.size();
                return false;
            }
        }
    }

    protected ValueObject getValueObject(int position) throws SQLException {
        if (this.rowPointer < 0) {
            throwError("Current position is before the first row", ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        if (this.rowPointer >= resultSet.size()) {
            throwError("Current position is after the last row", ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        ValueObject[] row = resultSet.get(this.rowPointer);
        if (position <= 0 || position > row.length) {
            throwError("No such column: " + position, ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        ValueObject vo = row[position - 1];
        this.lastGetWasNull = vo.isNull();
        return vo;
    }

    private void throwError(String message, ExceptionCode exceptionCode) throws SQLException {
        if (statement != null) {
            ExceptionMapper.throwException(new QueryException("Current position is before the first row", ExceptionCode.INVALID_PARAMETER_VALUE),
                    (MariaDbConnection) this.statement.getConnection(), this.statement);
        } else {
            throw new SQLException(message, exceptionCode.sqlState);
        }
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (this.statement == null) {
            return null;
        }
        return this.statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (this.statement != null) {
            this.statement.clearWarnings();
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClose();
        return rowPointer == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClose();
        if (dataFetchTime > 0) {
            return rowPointer >= resultSet.size() && resultSet.size() > 0;
        }
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClose();
        return dataFetchTime == 1 && rowPointer == 0 && resultSet.size() > 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClose();
        if (dataFetchTime > 0 && isEof) {
            return rowPointer == resultSet.size() - 1 && resultSet.size() > 0;
        } else if (streaming) {
            try {
                nextStreamingValue();
            } catch (IOException ioe) {
                throw new SQLException(ioe);
            } catch (QueryException queryException) {
                throw new SQLException(queryException);
            }
            return rowPointer == resultSet.size() - 1 && resultSet.size() > 0;
        }
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = -1;
        }
    }

    @Override
    public void afterLast() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = resultSet.size();
        }
    }

    @Override
    public boolean first() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = 0;
            return true;
        }
    }

    @Override
    public boolean last() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = resultSet.size() - 1;
            return true;
        }
    }

    @Override
    public int getRow() throws SQLException {
        checkClose();
        if (streaming) {
            return 0;
        }
        return rowPointer + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            if (row >= 0 && row <= resultSet.size()) {
                rowPointer = row - 1;
                return true;
            } else if (row < 0) {
                rowPointer = resultSet.size() + row;
            }
            return true;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            int newPos = rowPointer + rows;
            if (newPos > -1 && newPos <= resultSet.size()) {
                rowPointer = newPos;
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            if (rowPointer > -1) {
                rowPointer--;
                if (rowPointer == -1) {
                    return false;
                } else {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_UNKNOWN;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction == FETCH_REVERSE) {
            throw new SQLException("Invalid operation. Allowed direction are ResultSet.FETCH_FORWARD and ResultSet.FETCH_UNKNOWN");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        if (streaming && this.fetchSize == 0) {
            try {
                while (readNextValue(resultSet)) {
                    //fetch all results
                }
            } catch (IOException ioException) {
                throw new SQLException(ioException);
            } catch (QueryException queryException) {
                throw new SQLException(queryException);
            }

            dataFetchTime++;
            streaming = false;

        }
        this.fetchSize = fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return resultSetScrollType;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    private void checkClose() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Operation not permit on a closed resultset", "HY000");
        }
    }

    public boolean isCallableResult() {
        return callableResult;
    }

    public void setCallableResult(boolean callableResult) {
        this.callableResult = callableResult;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    /**
     * {inheritDoc}
     */
    public boolean wasNull() throws SQLException {
        return lastGetWasNull;
    }

    /**
     * {inheritDoc}
     */
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));

    }

    /**
     * {inheritDoc}
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getInputStream();
    }

    public String getString(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getString(cal);
    }

    /**
     * {inheritDoc}
     */
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getBinaryInputStream();
    }

    /**
     * {inheritDoc}
     */
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public int getInt(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getInt();
    }

    /**
     * {inheritDoc}
     */
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public long getLong(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getLong();
    }

    /**
     * {inheritDoc}
     */
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public float getFloat(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getFloat();
    }

    /**
     * {inheritDoc}
     */
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}
     */
    public double getDouble(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getDouble();
    }

    /**
     * {inheritDoc}
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    /**
     * {inheritDoc}
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getValueObject(columnIndex).getBigDecimal();
    }

    /**
     * {inheritDoc}
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getBigDecimal();
    }

    /**
     * {inheritDoc}
     */
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getValueObject(findColumn(columnLabel)).getBigDecimal();
    }

    /**
     * {inheritDoc}
     */
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getBytes();
    }

    /**
     * {inheritDoc}
     */
    public Date getDate(int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getDate(cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse column as date, was: \""
                    + getValueObject(columnIndex).getString()
                    + "\"", e);
        }
    }

    /**
     * {inheritDoc}
     */
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        try {
            return getValueObject(columnIndex).getDate(cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse as date");
        }
    }

    /**
     * {inheritDoc}
     */
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    /**
     * {inheritDoc}
     */
    public Time getTime(int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getTime(cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse column as time, was: \""
                    + getValueObject(columnIndex).getString()
                    + "\"", e);
        }
    }

    /**
     * {inheritDoc}
     */
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}
     */
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        try {
            return getValueObject(columnIndex).getTime(cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse time", e);
        }
    }

    /**
     * {inheritDoc}
     */
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }


    /**
     * {inheritDoc}
     */
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}
     */
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        try {
            Timestamp result = getValueObject(columnIndex).getTimestamp(cal);
            if (result == null) {
                return null;
            }
            return new Timestamp(result.getTime());
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse timestamp", e);
        }
    }

    /**
     * {inheritDoc}
     */
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    /**
     * {inheritDoc}
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getTimestamp(cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse column as timestamp, was: \""
                    + getValueObject(columnIndex).getString()
                    + "\"", e);
        }
    }

    /**
     * {inheritDoc}
     */
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getInputStream();
    }

    /**
     * {inheritDoc}
     */
    public String getCursorName() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Cursors not supported");
    }

    /**
     * {inheritDoc}
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        return new MariaDbResultSetMetaData(columnsInformation, dataTypeMappingFlags, returnTableAlias);
    }

    /**
     * {inheritDoc}
     */
    public Object getObject(int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getObject(dataTypeMappingFlags, cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not get object: " + e.getMessage(), "S1009", e);
        }
    }

    /**
     * {inheritDoc}
     */
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    /**
     * {inheritDoc}
     */
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel));
    }


    public <T> T getObject(int columnIndex, Class<T> arg1) throws SQLException {
        return (T) getObject(columnIndex);
    }

    public <T> T getObject(String columnLabel, Class<T> arg1) throws SQLException {
        return (T) getObject(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public int findColumn(String columnLabel) throws SQLException {
        return columnNameMap.getIndex(columnLabel) + 1;
    }

    /**
     * {inheritDoc}
     */
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String value = getValueObject(columnIndex).getString();
        if (value == null) {
            return null;
        }
        return new StringReader(value);
    }

    /**
     * {inheritDoc}
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    /**
     * {inheritDoc}
     */
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    /**
     * {inheritDoc}
     */
    public boolean rowUpdated() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Detecting row updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public boolean rowInserted() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Detecting inserts are not supported");
    }

    /**
     * {inheritDoc}
     */
    public boolean rowDeleted() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Row deletes are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNull(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNull(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBoolean(int columnIndex, boolean bool) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBoolean(String columnLabel, boolean value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateByte(int columnIndex, byte value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateByte(String columnLabel, byte value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateShort(int columnIndex, short value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateShort(String columnLabel, short value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateInt(int columnIndex, int value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateInt(String columnLabel, int value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateFloat(int columnIndex, float value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateFloat(String columnLabel, float value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateDouble(int columnIndex, double value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateDouble(String columnLabel, double value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBigDecimal(int columnIndex, BigDecimal value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBigDecimal(String columnLabel, BigDecimal value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateString(int columnIndex, String value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateString(String columnLabel, String value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBytes(int columnIndex, byte[] value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBytes(String columnLabel, byte[] value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateDate(int columnIndex, Date date) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateDate(String columnLabel, Date value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateTime(int columnIndex, Time time) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateTime(String columnLabel, Time value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateTimestamp(int columnIndex, Timestamp timeStamp) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateTimestamp(String columnLabel, Timestamp value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateAsciiStream(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateAsciiStream(String columnLabel, InputStream value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateAsciiStream(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBinaryStream(String columnLabel, InputStream value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBinaryStream(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBinaryStream(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateCharacterStream(int columnIndex, Reader value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateCharacterStream(int columnIndex, Reader value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateCharacterStream(int columnIndex, Reader value, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateObject(int columnIndex, Object value, int scaleOrLength) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateObject(int columnIndex, Object value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateObject(String columnLabel, Object value, int scaleOrLength) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateObject(String columnLabel, Object value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateLong(String columnLabel, long value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateLong(int columnIndex, long value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void insertRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void deleteRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void refreshRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Row refresh is not supported");
    }

    /**
     * {inheritDoc}
     */
    public void cancelRowUpdates() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void moveToInsertRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void moveToCurrentRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public Ref getRef(int columnIndex) throws SQLException {
        // TODO: figure out what REF's are and implement this method
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public Ref getRef(String columnLabel) throws SQLException {
        // TODO see getRef(int)
        throw ExceptionMapper.getFeatureNotSupportedException("Getting REFs not supported");
    }

    /**
     * {inheritDoc}
     */
    public Blob getBlob(int columnIndex) throws SQLException {
        byte[] bytes = getValueObject(columnIndex).getBytes();
        if (bytes == null) {
            return null;
        }
        return new MariaDbBlob(bytes);
    }

    /**
     * {inheritDoc}
     */
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public Clob getClob(int columnIndex) throws SQLException {
        byte[] bytes = getValueObject(columnIndex).getBytes();
        if (bytes == null) {
            return null;
        }
        return new MariaDbClob(bytes);
    }

    /**
     * {inheritDoc}
     */
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public Array getArray(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Arrays are not supported");
    }

    /**
     * {inheritDoc}
     */
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(getValueObject(columnIndex).getString());
        } catch (MalformedURLException e) {
            throw ExceptionMapper.getSqlException("Could not parse as URL");
        }
    }

    /**
     * {inheritDoc}
     */
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public void updateRef(int columnIndex, Ref ref) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateRef(String columnLabel, Ref ref) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBlob(int columnIndex, Blob blob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBlob(String columnLabel, Blob blob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateClob(int columnIndex, Clob clob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateClob(String columnLabel, Clob clob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateArray(int columnIndex, Array array) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateArray(String columnLabel, Array array) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public java.sql.RowId getRowId(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * {inheritDoc}
     */
    public java.sql.RowId getRowId(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateRowId(int columnIndex, java.sql.RowId rowId) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");

    }

    /**
     * {inheritDoc}
     */
    public void updateRowId(String columnLabel, java.sql.RowId rowId) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");

    }

    /**
     * {inheritDoc}
     */
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * {inheritDoc}
     */
    public void updateNString(int columnIndex, String nstring) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNString(String columnLabel, String nstring) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNClob(int columnIndex, NClob nclob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNClob(String columnLabel, NClob nclob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public NClob getNClob(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("NClobs are not supported");
    }

    /**
     * {inheritDoc}
     */
    public NClob getNClob(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("NClobs are not supported");
    }

    /**
     * {inheritDoc}
     */
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}
     */
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}
     */
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}
     */
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}
     */
    public String getNString(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("NString not supported");
    }

    /**
     * {inheritDoc}
     */
    public String getNString(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("NString not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNCharacterStream(int columnIndex, Reader value, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}
     */
    public boolean getBoolean(int index) throws SQLException {
        return getValueObject(index).getBoolean();
    }

    /**
     * {inheritDoc}
     */
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public byte getByte(int index) throws SQLException {
        return getValueObject(index).getByte();
    }

    /**
     * {inheritDoc}
     */
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}
     */
    public short getShort(int index) throws SQLException {
        return getValueObject(index).getShort();
    }

    /**
     * {inheritDoc}
     */
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    /**
     * {inheritDoc}
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public void setReturnTableAlias(boolean returnTableAlias) {
        this.returnTableAlias = returnTableAlias;
    }
}
