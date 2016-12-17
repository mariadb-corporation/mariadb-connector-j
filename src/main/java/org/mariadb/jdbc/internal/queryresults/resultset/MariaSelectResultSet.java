/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDB Ab.

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

import org.mariadb.jdbc.*;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.packet.Packet;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.ColumnNameMap;
import org.mariadb.jdbc.internal.queryresults.SingleExecutionResult;
import org.mariadb.jdbc.internal.stream.MariaDbInputStream;
import org.mariadb.jdbc.internal.util.ExceptionCode;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.*;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;

@SuppressWarnings("deprecation")
public class MariaSelectResultSet implements ResultSet {
    private static Logger logger = LoggerFactory.getLogger(MariaSelectResultSet.class);
    public static final MariaSelectResultSet EMPTY = createEmptyResultSet();

    public static final int TINYINT1_IS_BIT = 1;
    public static final int YEAR_IS_DATE_TYPE = 2;
    private static final Pattern isIntegerRegex = Pattern.compile("^-?\\d+\\.0+$");

    private Protocol protocol;
    private ReadPacketFetcher packetFetcher;
    private MariaDbInputStream inputStream;

    private MariaDbStatement statement;
    private RowPacket rowPacket;
    private ColumnInformation[] columnsInformation;

    private byte[] lastReusableArray = null;
    private boolean isEof;
    private boolean isBinaryEncoded;
    private int dataFetchTime;
    private boolean streaming;
    private int columnInformationLength;
    private List<byte[][]> resultSet;
    private int resultSetSize;
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
    public boolean callableResult;

    /**
     * Create Streaming resultset.
     *
     * @param columnInformation   column information
     * @param statement           statement
     * @param protocol            current protocol
     * @param fetcher             stream fetcher
     * @param isBinaryEncoded     is binary protocol ?
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param fetchSize           current fetch size
     * @param isCanHaveCallableResultset is it from a callableStatement ?
     */
    public MariaSelectResultSet(ColumnInformation[] columnInformation, MariaDbStatement statement, Protocol protocol,
                                ReadPacketFetcher fetcher, boolean isBinaryEncoded,
                                int resultSetScrollType, int fetchSize, boolean isCanHaveCallableResultset) {

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
        this.inputStream = packetFetcher.getInputStream();
        this.isEof = false;
        this.isBinaryEncoded = isBinaryEncoded;
        this.fetchSize = fetchSize;
        this.resultSetScrollType = resultSetScrollType;
        this.resultSet = new ArrayList<>();
        this.resultSetSize = 0;
        this.dataFetchTime = 0;
        this.rowPointer = -1;
        this.callableResult = isCanHaveCallableResultset;
    }

    /**
     * Create filled resultset.
     *
     * @param columnInformation   column information
     * @param resultSet           resultset
     * @param protocol            current protocol
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     */
    public MariaSelectResultSet(ColumnInformation[] columnInformation, List<byte[][]> resultSet, Protocol protocol,
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
        this.isBinaryEncoded = false;
        this.fetchSize = 1;
        this.resultSetScrollType = resultSetScrollType;
        this.resultSet = resultSet;
        this.resultSetSize = this.resultSet.size();
        this.dataFetchTime = 0;
        this.rowPointer = -1;
        this.callableResult = false;
    }

    /**
     * Create a result set from given data. Useful for creating "fake" resultsets for DatabaseMetaData, (one example is
     * MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param data                 - each element of this array represents a complete row in the ResultSet. Each value is given in its
     *                             string representation, as in MySQL text protocol, except boolean (BIT(1)) values that are represented
     *                             as "1" or "0" strings
     * @param protocol             protocol
     * @param findColumnReturnsOne - special parameter, used only in generated key result sets
     * @return resultset
     */
    public static ResultSet createGeneratedData(long[] data, Protocol protocol, boolean findColumnReturnsOne) {
        ColumnInformation[] columns = new ColumnInformation[1];
        columns[0] = ColumnInformation.create("insert_id", MariaDbType.BIGINT);

        List<byte[][]> rows = new ArrayList<>();
        for (long rowData : data) {
            if (rowData != 0) {
                byte[][] row = new byte[1][];
                row[0] = String.valueOf(rowData).getBytes();
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
     * @param data        - each element of this array represents a complete row in the ResultSet. Each value is given in its string representation,
     *                    as in MySQL text protocol, except boolean (BIT(1)) values that are represented as "1" or "0" strings
     * @param protocol    protocol
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
        List<byte[][]> rows = new ArrayList<>();
        for (String[] rowData : data) {
            byte[][] row = new byte[columnNameLength][];

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
                row[i] = bytes;
            }
            rows.add(row);
        }
        return new MariaSelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
    }

    private static MariaSelectResultSet createEmptyResultSet() {
        return new MariaSelectResultSet(new ColumnInformation[0], new ArrayList<byte[][]>(), null,
                TYPE_SCROLL_SENSITIVE);
    }

    /**
     * Initialize and fetch first value.
     *
     * @throws IOException    exception
     * @throws QueryException exception
     */
    public void initFetch() throws IOException, QueryException {
        if (isBinaryEncoded) {
            rowPacket = new BinaryRowPacket(columnsInformation, columnInformationLength);
        } else {
            rowPacket = new TextRowPacket(columnInformationLength);
        }
        if (fetchSize == 0 || resultSetScrollType != TYPE_FORWARD_ONLY) {
            fetchAllResults();
            streaming = false;
        } else {
            protocol.setActiveStreamingResult(this);
            resultSet = new ArrayList<>(fetchSize);
            nextStreamingValue();
            // streaming = true;
            /*
             * This is a hack related to MariaDBConnection.createStatement(), where scroll type is always set to TYPE_FORWARD_ONLY.
             * If fetchSize>0, streaming is always activated, which cause some seek functions (first, last, absolute) become disabled.
             * Setting streaming to false, keeps the seek functions functional.
             * This is also more consistent with this.setFetchSize(), which disables streaming too.
             * 
             * setFetchSize(Integer.MIN_VALUE) is used to signal streaming=true, so we still have to respect that.
             * but, MariaDbStatement.setFetchSize(Integer.MIN_VALUE), sets it to 1
             */
            if (fetchSize <= 1) {
                streaming = true;
            } else {
                streaming = false;
            }
        }
    }

    public boolean isBinaryEncoded() {
        return isBinaryEncoded;
    }

    private void fetchAllResults() throws IOException, QueryException {

        final List<byte[][]> valueObjects = new ArrayList<>();
        while (readNextValue(valueObjects)) {
            //fetch all results
        }
        dataFetchTime++;
        resultSet = valueObjects;
        this.resultSetSize = resultSet.size();
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

                resultSetSize = resultSet.size();

                //retrieve other results if needed
                if (protocolTmp.hasMoreResults() && this.statement != null) {
                    this.statement.getMoreResults();
                }

            } catch (IOException ioexception) {
                throw new QueryException("Could not close resultset : " + ioexception.getMessage(), -1, CONNECTION_EXCEPTION, ioexception);
            }
        } catch (QueryException queryException) {
            ExceptionMapper.throwException(queryException, null, this.statement);
        }

        dataFetchTime++;
        streaming = false;
    }


    private void nextStreamingValue() throws IOException, QueryException {

        resultSet.clear();
        //fetch maximum fetchSize results
        int fetchSizeTmp = fetchSize;
        while (fetchSizeTmp > 0 && readNextValue(resultSet)) {
            fetchSizeTmp--;
        }
        dataFetchTime++;
        this.resultSetSize = resultSet.size();
    }

    /**
     * Read next value.
     *
     * @param values values
     * @return true if have a new value
     * @throws IOException    exception
     * @throws QueryException exception
     */
    public boolean readNextValue(List<byte[][]> values) throws IOException, QueryException {
        int length = inputStream.readHeader();
        if (length < 0x00ffffff) {
            //There is only one packet.
            // we don't have to check for every read that packet size is enough to read another packet.
            //read directly from stream to avoid creating byte array and copy data afterward.

            int read = inputStream.read() & 0xff;
            if (logger.isTraceEnabled()) logger.trace("read packet data(part):0x" + Integer.valueOf(String.valueOf(read), 16));
            int remaining = length - 1;

            if (read == 255) { //ERROR packet
                protocol.setActiveStreamingResult(null);
                Buffer buffer = packetFetcher.getReusableBuffer(remaining, lastReusableArray);
                ErrorPacket errorPacket = new ErrorPacket(buffer, false);
                lastReusableArray = null;
                if (statement != null) {
                    throw new QueryException("(conn:" + statement.getServerThreadId() + ") " + errorPacket.getMessage(),
                            errorPacket.getErrorNumber(), errorPacket.getSqlState());
                } else {
                    throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
                }
            }

            if (read == 254 && remaining < 9) { //EOF packet

                Buffer buffer = packetFetcher.getReusableBuffer(remaining, lastReusableArray);
                protocol.setHasWarnings(((buffer.buf[0] & 0xff) + ((buffer.buf[1] & 0xff) << 8)) > 0);

                //force the more packet value when this is a callable output result.
                //There is always a OK packet after a callable output result, but mysql 5.6-7
                //is sending a bad "more result" flag (without setting more packet to true)
                //so force the value, since this will corrupt connection.
                protocol.setMoreResults(callableResult
                        || (((buffer.buf[2] & 0xff) + ((buffer.buf[3] & 0xff) << 8)) & ServerStatus.MORE_RESULTS_EXISTS) != 0,
                        isBinaryEncoded);
                if (!protocol.hasMoreResults()) {
                    if (protocol.getActiveStreamingResult() == this) protocol.setActiveStreamingResult(null);
                    protocol = null;
                    packetFetcher = null;
                    inputStream = null;
                }
                lastReusableArray = null;
                isEof = true;
                return false;
            }

            values.add(rowPacket.getRow(packetFetcher, inputStream, remaining, read));
            return true;
        }

        //if not possible read with standard packet
        Buffer buffer = packetFetcher.getReusableBuffer(length, lastReusableArray);
        lastReusableArray = buffer.buf;

        //is error Packet
        if (buffer.getByteAt(0) == Packet.ERROR) {
            protocol.setActiveStreamingResult(null);
            ErrorPacket errorPacket = new ErrorPacket(buffer);
            lastReusableArray = null;
            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        //is EOF stream
        if ((buffer.getByteAt(0) == Packet.EOF && buffer.limit < 9)) {
            if (protocol.getActiveStreamingResult() == this) {
                protocol.setActiveStreamingResult(null);
            }
            protocol.setHasWarnings(((buffer.buf[1] & 0xff) + ((buffer.buf[2] & 0xff) << 8)) > 0);
            protocol.setMoreResults(callableResult
                            || (((buffer.buf[3] & 0xff) + ((buffer.buf[4] & 0xff) << 8)) & ServerStatus.MORE_RESULTS_EXISTS) != 0,
                    isBinaryEncoded);
            protocol = null;
            packetFetcher = null;
            inputStream = null;
            isEof = true;
            lastReusableArray = null;
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

                            protocol.setHasWarnings(endOfFilePacket.getWarningCount() > 0);
                            protocol.setMoreResults((endOfFilePacket.getStatusFlags() & ServerStatus.MORE_RESULTS_EXISTS) != 0, isBinaryEncoded);
                            if (!protocol.hasMoreResults()) {
                                if (protocol.getActiveStreamingResult() == this) protocol.setActiveStreamingResult(null);
                            }
                            lastReusableArray = null;
                            isEof = true;
                        }
                    }

                    while (protocol.hasMoreResults()) {
                        protocol.getMoreResults(new SingleExecutionResult(statement, 0, true, callableResult));
                    }

                    if (protocol.getActiveStreamingResult() == this) protocol.setActiveStreamingResult(null);

                } catch (IOException ioexception) {
                    throw new QueryException("Could not close resultSet : " + ioexception.getMessage(), -1, CONNECTION_EXCEPTION, ioexception);
                }
            } catch (QueryException queryException) {
                ExceptionMapper.throwException(queryException, null, this.statement);
            } finally {
                protocol = null;
                packetFetcher = null;
                inputStream = null;
                lock.unlock();
            }
        }

        //clean releasing memory
        for (byte[][] bytes : resultSet) {
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = null;
            }
        }
        resultSet.clear();

        if (statement != null) {
            ((MariaDbStatement) statement).checkCloseOnCompletion(this);
            statement = null;
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed) throw new SQLException("Operation not permit on a closed resultset", "HY000");
        if (rowPointer < resultSetSize - 1) {
            rowPointer++;
            return true;
        } else {
            if (streaming) {
                if (isEof) {
                    return false;
                } else {
                    try {
                        nextStreamingValue();
                    } catch (IOException ioe) {
                        throw new SQLException("Server has closed the connection. If result set contain huge amount of data, Server expects client to"
                                + " read off the result set relatively fast. "
                                + "In this case, please consider increasing net_wait_timeout session variable."
                                + " / processing your result set faster (check Streaming result sets documentation for more information)", ioe);
                    } catch (QueryException queryException) {
                        throw new SQLException(queryException);
                    }
                    rowPointer = 0;
                    return resultSetSize > 0;
                }
            } else {
                rowPointer = resultSetSize;
                return false;
            }
        }
    }

    protected byte[] checkObjectRange(int position) throws SQLException {
        if (this.rowPointer < 0) {
            throwError("Current position is before the first row", ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        if (this.rowPointer >= resultSetSize) {
            throwError("Current position is after the last row", ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        byte[][] row = resultSet.get(this.rowPointer);
        if (position <= 0 || position > row.length) {
            throwError("No such column: " + position, ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        byte[] vo = row[position - 1];

        this.lastGetWasNull = isNull(vo, columnsInformation[position - 1].getType());
        return vo;
    }

    private void throwError(String message, ExceptionCode exceptionCode) throws SQLException {
        if (statement != null) {
            ExceptionMapper.throwException(new QueryException(message, ExceptionCode.INVALID_PARAMETER_VALUE),
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
        return (dataFetchTime > 0) ? rowPointer == -1 && resultSetSize > 0 : rowPointer == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClose();
        return dataFetchTime > 0 && rowPointer >= resultSetSize && resultSetSize > 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClose();
        return dataFetchTime == 1 && rowPointer == 0 && resultSetSize > 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClose();
        if (dataFetchTime > 0 && isEof) {
            return rowPointer == resultSetSize - 1 && resultSetSize > 0;
        } else if (streaming) {
            try {
                nextStreamingValue();
            } catch (IOException ioe) {
                throw new SQLException(ioe);
            } catch (QueryException queryException) {
                throw new SQLException(queryException);
            }
            return rowPointer == resultSetSize - 1 && resultSetSize > 0;
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
            rowPointer = resultSetSize;
        }
    }

    @Override
    public boolean first() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = 0;
            return resultSetSize > 0;
        }
    }

    @Override
    public boolean last() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = resultSetSize - 1;
            return rowPointer > 0;
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
            if (row >= 0 && row <= resultSetSize) {
                rowPointer = row - 1;
                return true;
            } else if (row < 0) {
                rowPointer = resultSetSize + row;
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
            if (newPos > -1 && newPos <= resultSetSize) {
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
                return rowPointer != -1;
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
        if (isClosed) {
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

    public MariaDbStatement getStatement() {
        return statement;
    }

    public void setStatement(MariaDbStatement statement) {
        this.statement = statement;
    }

    /**
     * {inheritDoc}.
     */
    public boolean wasNull() throws SQLException {
        return lastGetWasNull;
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));

    }

    /**
     * {inheritDoc}.
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getInputStream(checkObjectRange(columnIndex));
    }

    /**
     * {inheritDoc}.
     */
    public String getString(int columnIndex) throws SQLException {
        byte[] rawByte = checkObjectRange(columnIndex);
        return getString(rawByte, columnsInformation[columnIndex - 1], cal);
    }

    /**
     * {inheritDoc}.
     */
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    private String getString(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        return getString(rawBytes, columnInfo, null);
    }

    private String getString(byte[] rawBytes, ColumnInformation columnInfo, Calendar cal) throws SQLException {
        if (rawBytes == null) {
            return null;
        }

        switch (columnInfo.getType()) {
            case BIT:
                if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
                    return (rawBytes[0] == 0) ? "0" : "1";
                }
                break;
            case TINYINT:
                if (this.isBinaryEncoded) {
                    return String.valueOf(getTinyInt(rawBytes, columnInfo));
                }
                break;
            case SMALLINT:
                if (this.isBinaryEncoded) {
                    return String.valueOf(getSmallInt(rawBytes, columnInfo));
                }
                break;
            case INTEGER:
            case MEDIUMINT:
                if (this.isBinaryEncoded) {
                    return String.valueOf(getMediumInt(rawBytes, columnInfo));
                }
                break;
            case BIGINT:
                if (this.isBinaryEncoded) {
                    if (!columnInfo.isSigned()) {
                        return String.valueOf(getBigInteger(rawBytes, columnInfo));
                    }
                    return String.valueOf(getLong(rawBytes, columnInfo));
                }
                break;
            case DOUBLE:
                return String.valueOf(getDouble(rawBytes, columnInfo));
            case FLOAT:
                return String.valueOf(getFloat(rawBytes, columnInfo));
            case TIME:
                return getTimeString(rawBytes, columnInfo);
            case DATE:
                if (isBinaryEncoded) {
                    try {
                        Date date = getDate(rawBytes, columnInfo, cal);
                        return (date == null) ? null : date.toString();
                    } catch (ParseException e) {
                    }
                }
                break;
            case YEAR:
                if (options.yearIsDateType) {
                    try {
                        Date date = getDate(rawBytes, columnInfo, cal);
                        return (date == null) ? null : date.toString();
                    } catch (ParseException e) {
                        //eat exception
                    }
                }
                if (this.isBinaryEncoded) {
                    return String.valueOf(getSmallInt(rawBytes, columnInfo));
                }
                break;
            case TIMESTAMP:
            case DATETIME:
                try {
                    Timestamp timestamp = getTimestamp(rawBytes, columnInfo, cal);
                    if (timestamp == null) {
                        if (rawBytes != null && !this.isBinaryEncoded) {
                            return new String(rawBytes, StandardCharsets.UTF_8);
                        }
                        return null;
                    }
                    return timestamp.toString();
                } catch (ParseException e) {
                }
                break;
            case DECIMAL:
            case OLDDECIMAL:
                BigDecimal bigDecimal = getBigDecimal(rawBytes, columnInfo);
                return (bigDecimal == null ) ? null : bigDecimal.toString();
            case GEOMETRY:
                return new String(rawBytes);
            case NULL:
                return null;
            default:
                return new String(rawBytes, StandardCharsets.UTF_8);
        }
        return new String(rawBytes, StandardCharsets.UTF_8);
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        byte[] rawBytes = checkObjectRange(columnIndex);
        if (rawBytes == null) {
            return null;
        }
        return new ByteArrayInputStream(rawBytes);
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public int getInt(int columnIndex) throws SQLException {
        return getInt(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }


    /**
     * Get int from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return int
     */
    private int getInt(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return parseInt(rawBytes, columnInfo);
        } else {
            long value;
            switch (columnInfo.getType()) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt(rawBytes, columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt(rawBytes, columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = ((rawBytes[0] & 0xff)
                            + ((rawBytes[1] & 0xff) << 8)
                            + ((rawBytes[2] & 0xff) << 16)
                            + ((rawBytes[3] & 0xff) << 24));
                    if (columnInfo.isSigned()) {
                        return (int) value;
                    } else if (value < 0) {
                        value = value & 0xffffffffL;
                    }
                    break;
                case BIGINT:
                    value = getLong(rawBytes, columnInfo);
                    break;
                case FLOAT:
                    value = (long) getFloat(rawBytes, columnInfo);
                    break;
                case DOUBLE:
                    value = (long) getDouble(rawBytes, columnInfo);
                    break;
                default:
                    return parseInt(rawBytes, columnInfo);
            }
            rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value, columnInfo);
            return (int) value;
        }
    }

    /**
     * {inheritDoc}.
     */
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public long getLong(int columnIndex) throws SQLException {
        return getLong(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1]);
    }

    /**
     * Get long from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return long
     * @throws SQLException if any error occur
     */
    private long getLong(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return parseLong(rawBytes, columnInfo);
        } else {
            long value;
            switch (columnInfo.getType()) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt(rawBytes, columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt(rawBytes, columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt(rawBytes, columnInfo);
                    break;
                case BIGINT:
                    value = ((rawBytes[0] & 0xff)
                            + ((long) (rawBytes[1] & 0xff) << 8)
                            + ((long) (rawBytes[2] & 0xff) << 16)
                            + ((long) (rawBytes[3] & 0xff) << 24)
                            + ((long) (rawBytes[4] & 0xff) << 32)
                            + ((long) (rawBytes[5] & 0xff) << 40)
                            + ((long) (rawBytes[6] & 0xff) << 48)
                            + ((long) (rawBytes[7] & 0xff) << 56));
                    if (columnInfo.isSigned()) {
                        return value;
                    }
                    BigInteger unsignedValue = new BigInteger(1, new byte[]{(byte) (value >> 56),
                            (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                            (byte) (value >> 0)});
                    if (unsignedValue.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + unsignedValue + " is not in Long range", "22003", 1264);
                    }
                    return unsignedValue.longValue();
                case FLOAT:
                    Float floatValue = getFloat(rawBytes, columnInfo);
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + floatValue
                                + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = getDouble(rawBytes, columnInfo);
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + doubleValue
                                + " is not in Long range", "22003", 1264);
                    }
                    return doubleValue.longValue();
                default:
                    return parseLong(rawBytes, columnInfo);
            }
            rangeCheck(Long.class, Long.MIN_VALUE, Long.MAX_VALUE, value, columnInfo);
            return value;

        }
    }

    /**
     * {inheritDoc}.
     */
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public float getFloat(int columnIndex) throws SQLException {
        return getFloat(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1]);
    }

    /**
     * Get float from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return float
     * @throws SQLException id any error occur
     */
    private float getFloat(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            long value;
            switch (columnInfo.getType()) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt(rawBytes, columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt(rawBytes, columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt(rawBytes, columnInfo);
                    break;
                case BIGINT:
                    value = ((rawBytes[0] & 0xff)
                            + ((long) (rawBytes[1] & 0xff) << 8)
                            + ((long) (rawBytes[2] & 0xff) << 16)
                            + ((long) (rawBytes[3] & 0xff) << 24)
                            + ((long) (rawBytes[4] & 0xff) << 32)
                            + ((long) (rawBytes[5] & 0xff) << 40)
                            + ((long) (rawBytes[6] & 0xff) << 48)
                            + ((long) (rawBytes[7] & 0xff) << 56));
                    if (columnInfo.isSigned()) {
                        return value;
                    }
                    BigInteger unsignedValue = new BigInteger(1, new byte[]{(byte) (value >> 56),
                            (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                            (byte) (value >> 0)});
                    return unsignedValue.floatValue();
                case FLOAT:
                    int valueFloat = ((rawBytes[0] & 0xff)
                            + ((rawBytes[1] & 0xff) << 8)
                            + ((rawBytes[2] & 0xff) << 16)
                            + ((rawBytes[3] & 0xff) << 24));
                    return Float.intBitsToFloat(valueFloat);
                case DOUBLE:
                    return (float) getDouble(rawBytes, columnInfo);
                default:
                    return Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
            return Float.valueOf(String.valueOf(value));
        }
    }

    /**
     * {inheritDoc}.
     */
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}.
     */
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1]);
    }


    /**
     * Get double value from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return double
     * @throws SQLException id any error occur
     */
    private double getDouble(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            switch (columnInfo.getType()) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    return getTinyInt(rawBytes, columnInfo);
                case SMALLINT:
                case YEAR:
                    return getSmallInt(rawBytes, columnInfo);
                case INTEGER:
                case MEDIUMINT:
                    return getMediumInt(rawBytes, columnInfo);
                case BIGINT:
                    long valueLong = ((rawBytes[0] & 0xff)
                            + ((long) (rawBytes[1] & 0xff) << 8)
                            + ((long) (rawBytes[2] & 0xff) << 16)
                            + ((long) (rawBytes[3] & 0xff) << 24)
                            + ((long) (rawBytes[4] & 0xff) << 32)
                            + ((long) (rawBytes[5] & 0xff) << 40)
                            + ((long) (rawBytes[6] & 0xff) << 48)
                            + ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return valueLong;
                    } else {
                        return new BigInteger(1, new byte[]{(byte) (valueLong >> 56),
                                (byte) (valueLong >> 48), (byte) (valueLong >> 40), (byte) (valueLong >> 32),
                                (byte) (valueLong >> 24), (byte) (valueLong >> 16), (byte) (valueLong >> 8),
                                (byte) (valueLong >> 0)}).doubleValue();
                    }
                case FLOAT:
                    return getFloat(rawBytes, columnInfo);
                case DOUBLE:
                    long valueDouble = ((rawBytes[0] & 0xff)
                            + ((long) (rawBytes[1] & 0xff) << 8)
                            + ((long) (rawBytes[2] & 0xff) << 16)
                            + ((long) (rawBytes[3] & 0xff) << 24)
                            + ((long) (rawBytes[4] & 0xff) << 32)
                            + ((long) (rawBytes[5] & 0xff) << 40)
                            + ((long) (rawBytes[6] & 0xff) << 48)
                            + ((long) (rawBytes[7] & 0xff) << 56));
                    return Double.longBitsToDouble(valueDouble);
                default:
                    return Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }


    /**
     * Get BigDecimal from rax data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return Bigdecimal value
     * @throws SQLException id any error occur
     */
    private BigDecimal getBigDecimal(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            return new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            switch (columnInfo.getType()) {
                case BIT:
                    return BigDecimal.valueOf((long) rawBytes[0]);
                case TINYINT:
                    return BigDecimal.valueOf((long) getTinyInt(rawBytes, columnInfo));
                case SMALLINT:
                case YEAR:
                    return BigDecimal.valueOf((long) getSmallInt(rawBytes, columnInfo));
                case INTEGER:
                case MEDIUMINT:
                    return BigDecimal.valueOf(getMediumInt(rawBytes, columnInfo));
                case BIGINT:
                    long value = ((rawBytes[0] & 0xff)
                            + ((long) (rawBytes[1] & 0xff) << 8)
                            + ((long) (rawBytes[2] & 0xff) << 16)
                            + ((long) (rawBytes[3] & 0xff) << 24)
                            + ((long) (rawBytes[4] & 0xff) << 32)
                            + ((long) (rawBytes[5] & 0xff) << 40)
                            + ((long) (rawBytes[6] & 0xff) << 48)
                            + ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return new BigDecimal(String.valueOf(BigInteger.valueOf(value))).setScale(columnInfo.getDecimals());
                    } else {
                        return new BigDecimal(String.valueOf(new BigInteger(1, new byte[]{(byte) (value >> 56),
                                (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                                (byte) (value >> 0)}))).setScale(columnInfo.getDecimals());
                    }
                case FLOAT:
                    return BigDecimal.valueOf(getFloat(rawBytes, columnInfo));
                case DOUBLE:
                    return BigDecimal.valueOf(getDouble(rawBytes, columnInfo));
                default:
                    return new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }

    }

    /**
     * {inheritDoc}.
     */
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        return checkObjectRange(columnIndex);
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(int columnIndex) throws SQLException {
        try {
            return getDate(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse column as date, was: \""
                    + getString(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1])
                    + "\"", e);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        try {
            return getDate(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse as date");
        }
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }


    /**
     * Get date from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @param cal        session calendar
     * @return date
     * @throws ParseException if raw data cannot be parse
     */
    private Date getDate(byte[] rawBytes, ColumnInformation columnInfo, Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }

        if (!this.isBinaryEncoded) {
            String rawValue = new String(rawBytes, StandardCharsets.UTF_8);
            String zeroDate = "0000-00-00";

            if (rawValue.equals(zeroDate)) {
                return null;
            }

            SimpleDateFormat sdf;
            switch (columnInfo.getType()) {
                case TIMESTAMP:
                case DATETIME:
                    Timestamp timestamp = getTimestamp(rawBytes, columnInfo, cal);
                    if (timestamp == null) return null;
                    return new Date(timestamp.getTime());
                case TIME:
                    Time time = getTime(rawBytes, columnInfo, cal);
                    if (time == null) return null;
                    return new Date(time.getTime());
                case DATE:
                    return new Date(
                            Integer.parseInt(rawValue.substring(0, 4)) - 1900,
                            Integer.parseInt(rawValue.substring(5, 7)) - 1,
                            Integer.parseInt(rawValue.substring(8, 10))
                    );
                case YEAR:
                    int year = Integer.parseInt(rawValue);
                    if (rawBytes.length == 2 && columnInfo.getLength() == 2) {
                        if (year <= 69) {
                            year += 2000;
                        } else {
                            year += 1900;
                        }
                    }

                    return new Date(year - 1900, 0, 1);
                default:
                    sdf = new SimpleDateFormat("yyyy-MM-dd");
                    if (cal != null) {
                        sdf.setCalendar(cal);
                    }
            }
            java.util.Date utilDate = sdf.parse(rawValue);
            return new Date(utilDate.getTime());
        } else {
            return binaryDate(rawBytes, columnInfo, cal);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(int columnIndex) throws SQLException {
        try {
            return getTime(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse column as time, was: \""
                    + getString(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1])
                    + "\"", e);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}.
     */
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        try {
            return getTime(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse time", e);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    /**
     * Get time from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @param cal        session calendar
     * @return time value
     * @throws ParseException if raw data cannot be parse
     */
    private Time getTime(byte[] rawBytes, ColumnInformation columnInfo, Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        String raw = new String(rawBytes, StandardCharsets.UTF_8);
        String zeroDate = "0000-00-00";
        if (raw.equals(zeroDate)) {
            return null;
        }

        if (!this.isBinaryEncoded) {
            if (columnInfo.getType() == MariaDbType.TIMESTAMP || columnInfo.getType() == MariaDbType.DATETIME) {
                Timestamp timestamp = getTimestamp(rawBytes, columnInfo, cal);
                return (timestamp == null) ? null : new Time(timestamp.getTime());
            } else if (columnInfo.getType() == MariaDbType.DATE) {
                Calendar zeroCal = Calendar.getInstance();
                zeroCal.set(1970, 0, 1, 0, 0, 0);
                zeroCal.set(Calendar.MILLISECOND, 0);
                return new Time(zeroCal.getTimeInMillis());
            } else {
                if (!options.useLegacyDatetimeCode && (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3)) {
                    throw new ParseException("Time format \"" + raw + "\" incorrect, must be HH:mm:ss", 0);
                }
                boolean negate = raw.startsWith("-");
                if (negate) {
                    raw = raw.substring(1);
                }
                String[] rawPart = raw.split(":");
                if (rawPart.length == 3) {
                    int hour = Integer.parseInt(rawPart[0]);
                    int minutes = Integer.parseInt(rawPart[1]);
                    int seconds = Integer.parseInt(rawPart[2].substring(0, 2));
                    Calendar calendar = Calendar.getInstance();
                    if (options.useLegacyDatetimeCode) {
                        calendar.setLenient(true);
                    }
                    calendar.clear();
                    calendar.set(1970, 0, 1, (negate ? -1 : 1) * hour, minutes, seconds);
                    int nanoseconds = extractNanos(raw);
                    calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);

                    return new Time(calendar.getTimeInMillis());
                } else {
                    throw new ParseException(raw + " cannot be parse as time. time must have \"99:99:99\" format", 0);
                }
            }
        } else {
            return binaryTime(rawBytes, columnInfo, cal);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        try {
            return getTimestamp(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse timestamp", e);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            return getTimestamp(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not parse column as timestamp, was: \""
                    + getString(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1])
                    + "\"", e);
        }
    }

    /**
     * Get timeStamp from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @param cal        session calendar.
     * @return timestamp.
     * @throws ParseException if text value cannot be parse
     */
    private Timestamp getTimestamp(byte[] rawBytes, ColumnInformation columnInfo, Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            String rawValue = new String(rawBytes, StandardCharsets.UTF_8);
            if (rawValue.startsWith("0000-00-00 00:00:00")) return null;

            switch (columnInfo.getType()) {
                case TIME:
                    //time does not go after millisecond
                    Timestamp tt = new Timestamp(getTime(rawBytes, columnInfo, cal).getTime());
                    tt.setNanos(extractNanos(rawValue));
                    return tt;
                default:
                    try {
                        int hour = 0;
                        int minutes = 0;
                        int seconds = 0;

                        int year = Integer.parseInt(rawValue.substring(0, 4));
                        int month = Integer.parseInt(rawValue.substring(5, 7));
                        int day = Integer.parseInt(rawValue.substring(8, 10));
                        if (rawValue.length() >= 19) {
                            hour = Integer.parseInt(rawValue.substring(11, 13));
                            minutes = Integer.parseInt(rawValue.substring(14, 16));
                            seconds = Integer.parseInt(rawValue.substring(17, 19));
                        }
                        int nanoseconds = extractNanos(rawValue);
                        Timestamp timestamp;

                        Calendar calendar = options.useLegacyDatetimeCode ? Calendar.getInstance() : cal;

                        synchronized (calendar) {
                            calendar.set(Calendar.YEAR, year);
                            calendar.set(Calendar.MONTH, month - 1);
                            calendar.set(Calendar.DAY_OF_MONTH, day);
                            calendar.set(Calendar.HOUR_OF_DAY, hour);
                            calendar.set(Calendar.MINUTE, minutes);
                            calendar.set(Calendar.SECOND, seconds);
                            calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);
                            timestamp = new Timestamp(calendar.getTime().getTime());
                        }
                        timestamp.setNanos(nanoseconds);
                        return timestamp;
                    } catch (NumberFormatException n) {
                        throw new ParseException("Value \"" + rawValue + "\" cannot be parse as Timestamp", 0);
                    } catch (StringIndexOutOfBoundsException s) {
                        throw new ParseException("Value \"" + rawValue + "\" cannot be parse as Timestamp", 0);
                    }
            }
        } else {
            return binaryTimestamp(rawBytes, columnInfo, cal);
        }

    }

    /**
     * {inheritDoc}.
     */
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getInputStream(checkObjectRange(columnIndex));
    }

    /**
     * {inheritDoc}.
     */
    public String getCursorName() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Cursors not supported");
    }

    /**
     * {inheritDoc}.
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        return new MariaDbResultSetMetaData(columnsInformation, dataTypeMappingFlags, returnTableAlias);
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(int columnIndex) throws SQLException {
        try {
            return getObject(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], dataTypeMappingFlags, cal);
        } catch (ParseException e) {
            throw ExceptionMapper.getSqlException("Could not get object: " + e.getMessage(), "S1009", e);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        if (type == null) throw new SQLException("Class type cannot be null");
        if (type.equals(String.class)) {
            return (T) getString(parameterIndex);
        } else if (type.equals(Integer.class)) {
            getInt(parameterIndex);
        } else if (type.equals(Long.class)) {
            return (T) (Long) getLong(parameterIndex);
        } else if (type.equals(Short.class)) {
            return (T) (Short) getShort(parameterIndex);
        } else if (type.equals(Double.class)) {
            return (T) (Double) getDouble(parameterIndex);
        } else if (type.equals(Float.class)) {
            return (T) (Float) getFloat(parameterIndex);
        } else if (type.equals(Byte.class)) {
            return (T) (Byte) getByte(parameterIndex);
        } else if (type.equals(byte[].class)) {
            return (T) getBytes(parameterIndex);
        } else if (type.equals(Date.class)) {
            return (T) getDate(parameterIndex);
        } else if (type.equals(Time.class)) {
            return (T) getTime(parameterIndex);
        } else if (type.equals(Timestamp.class)) {
            return (T) getTimestamp(parameterIndex);
        } else if (type.equals(Boolean.class)) {
            return (T) (Boolean) getBoolean(parameterIndex);
        } else if (type.equals(Blob.class)) {
            return (T) getBlob(parameterIndex);
        } else if (type.equals(Clob.class)) {
            return (T) getClob(parameterIndex);
        } else if (type.equals(NClob.class)) {
            return (T) getNClob(parameterIndex);
        } else if (type.equals(InputStream.class)) {
            return (T) getBinaryStream(parameterIndex);
        } else if (type.equals(Reader.class)) {
            return (T) getCharacterStream(parameterIndex);
        } else if (type.equals(BigDecimal.class)) {
            return (T) getBigDecimal(parameterIndex);
        } else if (type.equals(BigInteger.class)) {
            return (T) getBigInteger(checkObjectRange(parameterIndex), columnsInformation[parameterIndex - 1]);
        } else if (type.equals(Clob.class)) {
            return (T) getClob(parameterIndex);
        }

        Object obj = getObject(parameterIndex);
        if (obj == null) return null;
        if (obj.getClass().isInstance(type)) {
            return (T) obj;
        } else {
            throw new SQLException("result cannot be cast as  '" + type.getName() + "' (is '" + obj.getClass().getName() + "'");
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> arg1) throws SQLException {
        return (T) getObject(findColumn(columnLabel));
    }

    /**
     * Get object value.
     *
     * @param rawBytes             bytes
     * @param columnInfo           current column information
     * @param dataTypeMappingFlags dataTypeflag (year is date or int, bit boolean or int,  ...)
     * @param cal                  session calendar
     * @return the object value.
     * @throws ParseException if data cannot be parse
     */
    private Object getObject(byte[] rawBytes, ColumnInformation columnInfo, int dataTypeMappingFlags, Calendar cal)
            throws SQLException, ParseException {
        if (rawBytes == null) {
            return null;
        }

        switch (columnInfo.getType()) {
            case BIT:
                if (columnInfo.getLength() == 1) {
                    return rawBytes[0] != 0;
                }
                return rawBytes;
            case TINYINT:
                if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
                    if (!this.isBinaryEncoded) {
                        return rawBytes[0] != '0';
                    } else {
                        return rawBytes[0] != 0;
                    }
                }
                return getInt(rawBytes, columnInfo);
            case INTEGER:
                if (!columnInfo.isSigned()) {
                    return getLong(rawBytes, columnInfo);
                }
                return getInt(rawBytes, columnInfo);
            case BIGINT:
                if (!columnInfo.isSigned()) {
                    return getBigInteger(rawBytes, columnInfo);
                }
                return getLong(rawBytes, columnInfo);
            case DOUBLE:
                return getDouble(rawBytes, columnInfo);
            case TIMESTAMP:
            case DATETIME:
                return getTimestamp(rawBytes, columnInfo, cal);
            case DATE:
                return getDate(rawBytes, columnInfo, cal);
            case VARCHAR:
                if (columnInfo.isBinary()) {
                    return rawBytes;
                }
                return getString(rawBytes, columnInfo);
            case DECIMAL:
                return getBigDecimal(rawBytes, columnInfo);
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
                return rawBytes;
            case NULL:
                return null;
            case YEAR:
                if ((dataTypeMappingFlags & YEAR_IS_DATE_TYPE) != 0) {
                    return getDate(rawBytes, columnInfo, cal);
                }
                return getShort(rawBytes, columnInfo);
            case SMALLINT:
            case MEDIUMINT:
                return getInt(rawBytes, columnInfo);
            case FLOAT:
                return getFloat(rawBytes, columnInfo);
            case TIME:
                return getTime(rawBytes, columnInfo, cal);
            case VARSTRING:
            case STRING:
                if (columnInfo.isBinary()) {
                    return rawBytes;
                }
                return getString(rawBytes, columnInfo);
            case OLDDECIMAL:
                return getString(rawBytes, columnInfo);
            case GEOMETRY:
                return rawBytes;
            case ENUM:
                break;
            case NEWDATE:
                break;
            case SET:
                break;
            default:
                break;
        }
        throw new RuntimeException(columnInfo.getType().toString());
    }


    /**
     * {inheritDoc}.
     */
    public int findColumn(String columnLabel) throws SQLException {
        return columnNameMap.getIndex(columnLabel) + 1;
    }

    /**
     * {inheritDoc}.
     */
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String value = getString(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1]);
        if (value == null) {
            return null;
        }
        return new StringReader(value);
    }

    /**
     * {inheritDoc}.
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    /**
     * {inheritDoc}.
     */
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public boolean rowUpdated() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Detecting row updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public boolean rowInserted() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Detecting inserts are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public boolean rowDeleted() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Row deletes are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNull(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNull(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBoolean(int columnIndex, boolean bool) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBoolean(String columnLabel, boolean value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateByte(int columnIndex, byte value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateByte(String columnLabel, byte value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateShort(int columnIndex, short value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateShort(String columnLabel, short value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateInt(int columnIndex, int value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateInt(String columnLabel, int value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateFloat(int columnIndex, float value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateFloat(String columnLabel, float value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateDouble(int columnIndex, double value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateDouble(String columnLabel, double value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBigDecimal(int columnIndex, BigDecimal value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBigDecimal(String columnLabel, BigDecimal value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateString(int columnIndex, String value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateString(String columnLabel, String value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBytes(int columnIndex, byte[] value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBytes(String columnLabel, byte[] value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateDate(int columnIndex, Date date) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateDate(String columnLabel, Date value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateTime(int columnIndex, Time time) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateTime(String columnLabel, Time value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateTimestamp(int columnIndex, Timestamp timeStamp) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateTimestamp(String columnLabel, Timestamp value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(String columnLabel, InputStream value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(String columnLabel, InputStream value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(int columnIndex, Reader value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(int columnIndex, Reader value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(int columnIndex, Reader value, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(int columnIndex, Object value, int scaleOrLength) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(int columnIndex, Object value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(String columnLabel, Object value, int scaleOrLength) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(String columnLabel, Object value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateLong(String columnLabel, long value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateLong(int columnIndex, long value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void insertRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void deleteRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void refreshRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Row refresh is not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void cancelRowUpdates() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void moveToInsertRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void moveToCurrentRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public Ref getRef(int columnIndex) throws SQLException {
        // TODO: figure out what REF's are and implement this method
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public Ref getRef(String columnLabel) throws SQLException {
        // TODO see getRef(int)
        throw ExceptionMapper.getFeatureNotSupportedException("Getting REFs not supported");
    }

    /**
     * {inheritDoc}.
     */
    public Blob getBlob(int columnIndex) throws SQLException {
        byte[] bytes = checkObjectRange(columnIndex);
        if (bytes == null) {
            return null;
        }
        return new MariaDbBlob(bytes);
    }

    /**
     * {inheritDoc}.
     */
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Clob getClob(int columnIndex) throws SQLException {
        byte[] bytes = checkObjectRange(columnIndex);
        if (bytes == null) {
            return null;
        }
        return new MariaDbClob(bytes);
    }

    /**
     * {inheritDoc}.
     */
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Array getArray(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Arrays are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(getString(checkObjectRange(columnIndex), columnsInformation[columnIndex - 1], cal));
        } catch (MalformedURLException e) {
            throw ExceptionMapper.getSqlException("Could not parse as URL");
        }
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public void updateRef(int columnIndex, Ref ref) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateRef(String columnLabel, Ref ref) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(int columnIndex, Blob blob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(String columnLabel, Blob blob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(int columnIndex, Clob clob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(String columnLabel, Clob clob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateArray(int columnIndex, Array array) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateArray(String columnLabel, Array array) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public RowId getRowId(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * {inheritDoc}.
     */
    public RowId getRowId(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateRowId(int columnIndex, RowId rowId) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");

    }

    /**
     * {inheritDoc}.
     */
    public void updateRowId(String columnLabel, RowId rowId) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");

    }

    /**
     * {inheritDoc}.
     */
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * {inheritDoc}.
     */
    public void updateNString(int columnIndex, String nstring) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNString(String columnLabel, String nstring) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(int columnIndex, NClob nclob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(String columnLabel, NClob nclob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public NClob getNClob(int columnIndex) throws SQLException {
        byte[] bytes = checkObjectRange(columnIndex);
        if (bytes == null) return null;
        return new MariaDbClob(bytes);
    }

    /**
     * {inheritDoc}.
     */
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * {inheritDoc}.
     */
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    /**
     * {inheritDoc}.
     */
    public String getNString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public void updateNCharacterStream(int columnIndex, Reader value, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * {inheritDoc}.
     */
    public boolean getBoolean(int index) throws SQLException {
        return getBoolean(checkObjectRange(index), columnsInformation[index - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }


    /**
     * Get boolean value from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return boolean
     * @throws SQLException id any error occur
     */
    private boolean getBoolean(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return false;
        }
        if (!this.isBinaryEncoded) {
            if (rawBytes.length == 1 && rawBytes[0] == 0) {
                return false;
            }
            final String rawVal = new String(rawBytes, StandardCharsets.UTF_8);
            return !("false".equals(rawVal) || "0".equals(rawVal));
        } else {
            switch (columnInfo.getType()) {
                case BIT:
                    return rawBytes[0] != 0;
                case TINYINT:
                    return getTinyInt(rawBytes, columnInfo) != 0;
                case SMALLINT:
                case YEAR:
                    return getSmallInt(rawBytes, columnInfo) != 0;
                case INTEGER:
                case MEDIUMINT:
                    return getMediumInt(rawBytes, columnInfo) != 0;
                case BIGINT:
                    return getLong(rawBytes, columnInfo) != 0;
                case FLOAT:
                    return getFloat(rawBytes, columnInfo) != 0;
                case DOUBLE:
                    return getDouble(rawBytes, columnInfo) != 0;
                default:
                    final String rawVal = new String(rawBytes, StandardCharsets.UTF_8);
                    return !("false".equals(rawVal) || "0".equals(rawVal));
            }
        }
    }

    /**
     * {inheritDoc}.
     */
    public byte getByte(int index) throws SQLException {
        return getByte(checkObjectRange(index), columnsInformation[index - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    /**
     * Get byte from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return byte
     * @throws SQLException id any error occur
     */
    private byte getByte(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            if (columnInfo.getType() == MariaDbType.BIT) {
                return rawBytes[0];
            }
            return parseByte(rawBytes, columnInfo);
        } else {
            long value;
            switch (columnInfo.getType()) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt(rawBytes, columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt(rawBytes, columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt(rawBytes, columnInfo);
                    break;
                case BIGINT:
                    value = getLong(rawBytes, columnInfo);
                    break;
                case FLOAT:
                    value = (long) getFloat(rawBytes, columnInfo);
                    break;
                case DOUBLE:
                    value = (long) getDouble(rawBytes, columnInfo);
                    break;
                default:
                    return parseByte(rawBytes, columnInfo);
            }
            rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, value, columnInfo);
            return (byte) value;
        }
    }

    /**
     * {inheritDoc}.
     */
    public short getShort(int index) throws SQLException {
        return getShort(checkObjectRange(index), columnsInformation[index - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    /**
     * Get short from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return short
     * @throws SQLException exception
     * @throws SQLException id any error occur
     */
    private short getShort(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return parseShort(rawBytes, columnInfo);
        } else {
            long value;
            switch (columnInfo.getType()) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt(rawBytes, columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = ((rawBytes[0] & 0xff) + ((rawBytes[1] & 0xff) << 8));
                    if (columnInfo.isSigned()) {
                        return (short) value;
                    }
                    value = value & 0xffff;
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt(rawBytes, columnInfo);
                    break;
                case BIGINT:
                    value = getLong(rawBytes, columnInfo);
                    break;
                case FLOAT:
                    value = (long) getFloat(rawBytes, columnInfo);
                    break;
                case DOUBLE:
                    value = (long) getDouble(rawBytes, columnInfo);
                    break;
                default:
                    return parseShort(rawBytes, columnInfo);
            }
            rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, value, columnInfo);
            return (short) value;
        }
    }

    /**
     * {inheritDoc}.
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    /**
     * {inheritDoc}.
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public void setReturnTableAlias(boolean returnTableAlias) {
        this.returnTableAlias = returnTableAlias;
    }

    private String getTimeString(byte[] rawBytes, ColumnInformation columnInfo) {
        if (rawBytes == null) return null;
        if (rawBytes.length == 0) {
            // binary send 00:00:00 as 0.
            if (columnInfo.getDecimals() == 0) {
                return "00:00:00";
            } else {
                String value = "00:00:00.";
                int decimal = columnInfo.getDecimals();
                while (decimal-- > 0) value += "0";
                return value;
            }
        }
        String rawValue = new String(rawBytes, StandardCharsets.UTF_8);
        if ("0000-00-00".equals(rawValue)) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            if (options.maximizeMysqlCompatibility && options.useLegacyDatetimeCode && rawValue.indexOf(".") > 0) {
                return rawValue.substring(0, rawValue.indexOf("."));
            }
            return rawValue;
        }
        int day = ((rawBytes[1] & 0xff)
                | ((rawBytes[2] & 0xff) << 8)
                | ((rawBytes[3] & 0xff) << 16)
                | ((rawBytes[4] & 0xff) << 24));
        int hour = rawBytes[5];
        int timeHour = hour + day * 24;

        String hourString;
        if (timeHour < 10) {
            hourString = "0" + timeHour;
        } else {
            hourString = Integer.toString(timeHour);
        }

        String minuteString;
        int minutes = rawBytes[6];
        if (minutes < 10) {
            minuteString = "0" + minutes;
        } else {
            minuteString = Integer.toString(minutes);
        }

        String secondString;
        int seconds = rawBytes[7];
        if (seconds < 10) {
            secondString = "0" + seconds;
        } else {
            secondString = Integer.toString(seconds);
        }

        int microseconds = 0;
        if (rawBytes.length > 8) {
            microseconds = ((rawBytes[8] & 0xff)
                    | (rawBytes[9] & 0xff) << 8
                    | (rawBytes[10] & 0xff) << 16
                    | (rawBytes[11] & 0xff) << 24);
        }

        String microsecondString = Integer.toString(microseconds);
        while (microsecondString.length() < 6) {
            microsecondString = "0" + microsecondString;
        }
        boolean negative = (rawBytes[0] == 0x01);
        return (negative ? "-" : "") + (hourString + ":" + minuteString + ":" + secondString + "." + microsecondString);
    }


    private void rangeCheck(Object className, long minValue, long maxValue, long value, ColumnInformation columnInfo) throws SQLException {
        if (value < minValue || value > maxValue) {
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value + " is not in "
                    + className + " range", "22003", 1264);
        }
    }

    private int getTinyInt(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        int value = rawBytes[0];
        if (!columnInfo.isSigned()) {
            value = (rawBytes[0] & 0xff);
        }
        return value;
    }

    private int getSmallInt(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        int value = ((rawBytes[0] & 0xff) + ((rawBytes[1] & 0xff) << 8));
        if (!columnInfo.isSigned()) {
            return value & 0xffff;
        }
        //short cast here is important : -1 will be received as -1, -1 -> 65535
        return (short) value;
    }

    private long getMediumInt(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        long value = ((rawBytes[0] & 0xff)
                + ((rawBytes[1] & 0xff) << 8)
                + ((rawBytes[2] & 0xff) << 16)
                + ((rawBytes[3] & 0xff) << 24));
        if (!columnInfo.isSigned()) {
            value = value & 0xffffffffL;
        }
        return value;
    }


    private byte parseByte(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        try {
            switch (columnInfo.getType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Byte.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Byte range", "22003", 1264);
                    }
                    return floatValue.byteValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Byte.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Byte range", "22003", 1264);
                    }
                    return doubleValue.byteValue();
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                    long result = 0;
                    int length = rawBytes.length;
                    boolean negate = false;
                    int begin = 0;
                    if (length > 0 && rawBytes[0] == 45) { //minus sign
                        negate = true;
                        begin = 1;
                    }
                    for (; begin < length; begin++) {
                        result = result * 10 + rawBytes[begin] - 48;
                    }
                    result = (negate ? -1 * result : result);
                    rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, result, columnInfo);
                    return (byte) result;
                default:
                    return Byte.parseByte(new String(rawBytes, StandardCharsets.UTF_8));
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getByte with a database decimal value
            //retrying without the decimal part.
            String value = new String(rawBytes, StandardCharsets.UTF_8);
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Byte.parseByte(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException nfee) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Byte range",
                    "22003", 1264);
        }
    }

    private short parseShort(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        try {
            switch (columnInfo.getType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Short.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Short range", "22003", 1264);
                    }
                    return floatValue.shortValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Short.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Short range", "22003", 1264);
                    }
                    return doubleValue.shortValue();
                case BIT:
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                    long result = 0;
                    int length = rawBytes.length;
                    boolean negate = false;
                    int begin = 0;
                    if (length > 0 && rawBytes[0] == 45) { //minus sign
                        negate = true;
                        begin = 1;
                    }
                    for (; begin < length; begin++) {
                        result = result * 10 + rawBytes[begin] - 48;
                    }
                    result = (negate ? -1 * result : result);
                    rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, result, columnInfo);
                    return (short) result;
                default:
                    return Short.parseShort(new String(rawBytes, StandardCharsets.UTF_8));
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getInt with a database decimal value
            //retrying without the decimal part.
            String value = new String(rawBytes, StandardCharsets.UTF_8);
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Short.parseShort(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException numberFormatException) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Short range", "22003", 1264);
        }
    }


    private int parseInt(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        try {
            switch (columnInfo.getType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Integer.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Integer range", "22003", 1264);
                    }
                    return floatValue.intValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Integer.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Integer range", "22003", 1264);
                    }
                    return doubleValue.intValue();
                case BIT:
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                case BIGINT:
                    long result = 0;
                    int length = rawBytes.length;
                    boolean negate = false;
                    int begin = 0;
                    if (length > 0 && rawBytes[0] == 45) { //minus sign
                        negate = true;
                        begin = 1;
                    }
                    for (; begin < length; begin++) {
                        result = result * 10 + rawBytes[begin] - 48;
                    }
                    //specific for BIGINT : if value > Long.MAX_VALUE will become negative.
                    if (result < 0) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Integer range", "22003", 1264);
                    }
                    result = (negate ? -1 * result : result);
                    rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, result, columnInfo);
                    return (int) result;
                default:
                    return Integer.parseInt(new String(rawBytes, StandardCharsets.UTF_8));
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getInt with a database decimal value
            //retrying without the decimal part.
            String value = new String(rawBytes, StandardCharsets.UTF_8);
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Integer.parseInt(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException numberFormatException) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Integer range", "22003", 1264);
        }
    }

    private long parseLong(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        try {
            switch (columnInfo.getType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Long range", "22003", 1264);
                    }
                    return doubleValue.longValue();
                case BIT:
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                case BIGINT:
                    long result = 0;
                    int length = rawBytes.length;
                    boolean negate = false;
                    int begin = 0;
                    if (length > 0 && rawBytes[0] == 45) { //minus sign
                        negate = true;
                        begin = 1;
                    }
                    for (; begin < length; begin++) {
                        result = result * 10 + rawBytes[begin] - 48;
                    }
                    //specific for BIGINT : if value > Long.MAX_VALUE , will become negative until -1
                    if (result < 0) {
                        //CONJ-399 : handle specifically Long.MIN_VALUE that has absolute value +1 compare to LONG.MAX_VALUE
                        if (result == Long.MIN_VALUE && negate) return Long.MIN_VALUE;
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(rawBytes, StandardCharsets.UTF_8)
                                + " is not in Long range", "22003", 1264);
                    }
                    return (negate ? -1 * result : result);
                default:
                    return Long.parseLong(new String(rawBytes, StandardCharsets.UTF_8));
            }

        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getlong with a database decimal value
            //retrying without the decimal part.
            String value = new String(rawBytes, StandardCharsets.UTF_8);
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Long.parseLong(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException nfee) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Long range", "22003", 1264);
        }
    }

    /**
     * Get BigInteger from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return bigInteger
     * @throws SQLException exception
     */
    private BigInteger getBigInteger(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {
        if (rawBytes == null) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            return new BigInteger(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            switch (columnInfo.getType()) {
                case BIT:
                    return BigInteger.valueOf((long) rawBytes[0]);
                case TINYINT:
                    return BigInteger.valueOf((long) (columnInfo.isSigned() ? rawBytes[0] : (rawBytes[0] & 0xff)));
                case SMALLINT:
                case YEAR:
                    short valueShort = (short) ((rawBytes[0] & 0xff) | ((rawBytes[1] & 0xff) << 8));
                    return BigInteger.valueOf((long) (columnInfo.isSigned() ? valueShort : (valueShort & 0xffff)));
                case INTEGER:
                case MEDIUMINT:
                    int valueInt = ((rawBytes[0] & 0xff)
                            + ((rawBytes[1] & 0xff) << 8)
                            + ((rawBytes[2] & 0xff) << 16)
                            + ((rawBytes[3] & 0xff) << 24));
                    return BigInteger.valueOf(((columnInfo.isSigned()) ? valueInt : (valueInt >= 0) ? valueInt : valueInt & 0xffffffffL));
                case BIGINT:
                    long value = ((rawBytes[0] & 0xff)
                            + ((long) (rawBytes[1] & 0xff) << 8)
                            + ((long) (rawBytes[2] & 0xff) << 16)
                            + ((long) (rawBytes[3] & 0xff) << 24)
                            + ((long) (rawBytes[4] & 0xff) << 32)
                            + ((long) (rawBytes[5] & 0xff) << 40)
                            + ((long) (rawBytes[6] & 0xff) << 48)
                            + ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return BigInteger.valueOf(value);
                    } else {
                        return new BigInteger(1, new byte[]{(byte) (value >> 56),
                                (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                                (byte) (value >> 0)});
                    }
                case FLOAT:
                    return BigInteger.valueOf((long) getFloat(rawBytes, columnInfo));
                case DOUBLE:
                    return BigInteger.valueOf((long) getDouble(rawBytes, columnInfo));
                default:
                    return new BigInteger(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }

    }


    private Date binaryDate(byte[] rawBytes, ColumnInformation columnInfo, Calendar cal) throws ParseException {
        switch (columnInfo.getType()) {
            case TIMESTAMP:
            case DATETIME:
                Timestamp timestamp = getTimestamp(rawBytes, columnInfo, cal);
                return (timestamp == null) ? null : new Date(timestamp.getTime());
            default:
                if (rawBytes.length == 0) {
                    return null;
                }

                int year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);

                if (rawBytes.length == 2 && columnInfo.getLength() == 2) {
                    //YEAR(2) - deprecated
                    if (year <= 69) {
                        year += 2000;
                    } else {
                        year += 1900;
                    }
                }

                int month = 1;
                int day = 1;

                if (rawBytes.length >= 4) {
                    month = rawBytes[2];
                    day = rawBytes[3];
                }

                Calendar calendar = Calendar.getInstance();

                Date dt;
                synchronized (calendar) {
                    calendar.clear();
                    calendar.set(Calendar.YEAR, year);
                    calendar.set(Calendar.MONTH, month - 1);
                    calendar.set(Calendar.DAY_OF_MONTH, day);
                    calendar.set(Calendar.HOUR_OF_DAY, 0);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.MILLISECOND, 0);
                    dt = new Date(calendar.getTimeInMillis());
                }
                return dt;
        }
    }

    private Time binaryTime(byte[] rawBytes, ColumnInformation columnInfo, Calendar cal) throws ParseException {
        switch (columnInfo.getType()) {
            case TIMESTAMP:
            case DATETIME:
                Timestamp ts = binaryTimestamp(rawBytes, columnInfo, cal);
                return (ts == null) ? null : new Time(ts.getTime());
            case DATE:
                Calendar tmpCalendar = Calendar.getInstance();
                tmpCalendar.clear();
                tmpCalendar.set(1970, 0, 1, 0, 0, 0);
                tmpCalendar.set(Calendar.MILLISECOND, 0);
                return new Time(tmpCalendar.getTimeInMillis());
            default:
                Calendar calendar = Calendar.getInstance();
                calendar.clear();
                int day = 0;
                int hour = 0;
                int minutes = 0;
                int seconds = 0;
                boolean negate = false;
                if (rawBytes.length > 0) {
                    negate = (rawBytes[0] & 0xff) == 0x01;
                }
                if (rawBytes.length > 4) {
                    day = ((rawBytes[1] & 0xff)
                            + ((rawBytes[2] & 0xff) << 8)
                            + ((rawBytes[3] & 0xff) << 16)
                            + ((rawBytes[4] & 0xff) << 24));
                }
                if (rawBytes.length > 7) {
                    hour = rawBytes[5];
                    minutes = rawBytes[6];
                    seconds = rawBytes[7];
                }
                calendar.set(1970, 0, ((negate ? -1 : 1) * day) + 1, (negate ? -1 : 1) * hour, minutes, seconds);

                int nanoseconds = 0;
                if (rawBytes.length > 8) {
                    nanoseconds = ((rawBytes[8] & 0xff)
                            + ((rawBytes[9] & 0xff) << 8)
                            + ((rawBytes[10] & 0xff) << 16)
                            + ((rawBytes[11] & 0xff) << 24));
                }

                calendar.set(Calendar.MILLISECOND, nanoseconds / 1000);

                return new Time(calendar.getTimeInMillis());
        }
    }


    private Timestamp binaryTimestamp(byte[] rawBytes, ColumnInformation columnInfo, Calendar cal) throws ParseException {
        if (rawBytes.length == 0) {
            return null;
        }
        int year;
        int month;
        int day = 0;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        if (columnInfo.getType() == MariaDbType.TIME) {
            Calendar calendar = Calendar.getInstance();
            calendar.clear();

            boolean negate = false;
            if (rawBytes.length > 0) {
                negate = (rawBytes[0] & 0xff) == 0x01;
            }
            if (rawBytes.length > 4) {
                day = ((rawBytes[1] & 0xff)
                        + ((rawBytes[2] & 0xff) << 8)
                        + ((rawBytes[3] & 0xff) << 16)
                        + ((rawBytes[4] & 0xff) << 24));
            }
            if (rawBytes.length > 7) {
                hour = rawBytes[5];
                minutes = rawBytes[6];
                seconds = rawBytes[7];
            }

            if (rawBytes.length > 8) {
                microseconds = ((rawBytes[8] & 0xff)
                        + ((rawBytes[9] & 0xff) << 8)
                        + ((rawBytes[10] & 0xff) << 16)
                        + ((rawBytes[11] & 0xff) << 24));
            }

            calendar.set(1970, 0, ((negate ? -1 : 1) * day) + 1, (negate ? -1 : 1) * hour, minutes, seconds);
            Timestamp tt = new Timestamp(calendar.getTimeInMillis());
            tt.setNanos(microseconds * 1000);
            return tt;
        } else {
            year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);
            month = rawBytes[2];
            day = rawBytes[3];
            if (rawBytes.length > 4) {
                hour = rawBytes[4];
                minutes = rawBytes[5];
                seconds = rawBytes[6];

                if (rawBytes.length > 7) {
                    microseconds = ((rawBytes[7] & 0xff)
                            + ((rawBytes[8] & 0xff) << 8)
                            + ((rawBytes[9] & 0xff) << 16)
                            + ((rawBytes[10] & 0xff) << 24));
                }
            }
        }

        Calendar calendar = options.useLegacyDatetimeCode ? Calendar.getInstance() : cal;
        Timestamp tt;
        synchronized (calendar) {
            calendar.set(year, month - 1, day, hour, minutes, seconds);
            tt = new Timestamp(calendar.getTimeInMillis());
        }
        tt.setNanos(microseconds * 1000);
        return tt;
    }

    private int extractNanos(String timestring) throws ParseException {
        int index = timestring.indexOf('.');
        if (index == -1) {
            return 0;
        }
        int nanos = 0;
        for (int i = index + 1; i < index + 10; i++) {
            int digit;
            if (i >= timestring.length()) {
                digit = 0;
            } else {
                char value = timestring.charAt(i);
                if (value < '0' || value > '9') {
                    throw new ParseException("cannot parse subsecond part in timestamp string '" + timestring + "'", i);
                }
                digit = value - '0';
            }
            nanos = nanos * 10 + digit;
        }
        return nanos;
    }


    /**
     * Get inputStream value from raw data.
     * @param rawBytes rowdata
     * @return inputStream
     */
    public InputStream getInputStream(byte[] rawBytes) {
        if (rawBytes == null) {
            return null;
        }
        return new ByteArrayInputStream(new String(rawBytes, StandardCharsets.UTF_8).getBytes());
    }

    static final String zeroTimestamp = "0000-00-00 00:00:00";
    static final String zeroDate = "0000-00-00";

    /**
     * Is data null.
     *
     * @param rawBytes bytes
     * @param dataType field datatype
     * @return true if data is null
     */
    private boolean isNull(byte[] rawBytes, MariaDbType dataType) {
        return (rawBytes == null
                || (isBinaryEncoded && ((dataType == MariaDbType.DATE || dataType == MariaDbType.TIMESTAMP || dataType == MariaDbType.DATETIME)
                && rawBytes.length == 0))
                || (!isBinaryEncoded && ((dataType == MariaDbType.TIMESTAMP || dataType == MariaDbType.DATETIME)
                && zeroTimestamp.equals(new String(rawBytes, StandardCharsets.UTF_8))))
                || (!isBinaryEncoded && (dataType == MariaDbType.DATE && zeroDate.equals(new String(rawBytes, StandardCharsets.UTF_8)))));
    }


}
