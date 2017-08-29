/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */


package org.mariadb.jdbc.internal.com.read.resultset;

import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.MariaDbResultSetMetaData;
import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.read.ErrorPacket;
import org.mariadb.jdbc.internal.com.read.dao.ColumnNameMap;
import org.mariadb.jdbc.internal.com.read.dao.Results;
import org.mariadb.jdbc.internal.com.read.resultset.rowprotocol.BinaryRowProtocol;
import org.mariadb.jdbc.internal.com.read.resultset.rowprotocol.RowProtocol;
import org.mariadb.jdbc.internal.com.read.resultset.rowprotocol.TextRowProtocol;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.input.StandardPacketInputStream;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.mariadb.jdbc.internal.com.Packet.EOF;
import static org.mariadb.jdbc.internal.com.Packet.ERROR;
import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;
import static org.mariadb.jdbc.internal.util.constant.ServerStatus.MORE_RESULTS_EXISTS;
import static org.mariadb.jdbc.internal.util.constant.ServerStatus.PS_OUT_PARAMETERS;

@SuppressWarnings({"deprecation", "BigDecimalMethodWithoutRoundingCalled",
        "StatementWithEmptyBody", "SynchronizationOnLocalVariableOrMethodParameter"})
public class SelectResultSet implements ResultSet {
    private static final String NOT_UPDATABLE_ERROR = "Updates are not supported when using ResultSet.CONCUR_READ_ONLY";
    
    private static final DateTimeFormatter TEXT_LOCAL_DATE_TIME;
    private static final DateTimeFormatter TEXT_OFFSET_DATE_TIME;
    private static final DateTimeFormatter TEXT_ZONED_DATE_TIME;

    private static final int BIT_LAST_FIELD_NOT_NULL = 0b000000;
    private static final int BIT_LAST_FIELD_NULL     = 0b000001;
    private static final int BIT_LAST_ZERO_DATE      = 0b000010;

    public static final int TINYINT1_IS_BIT = 1;
    public static final int YEAR_IS_DATE_TYPE = 2;
    private static final ColumnInformation[] INSERT_ID_COLUMNS;
    private static final Pattern isIntegerRegex = Pattern.compile("^-?\\d+\\.[0-9]+$");
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    static {
        INSERT_ID_COLUMNS = new ColumnInformation[1];
        INSERT_ID_COLUMNS[0] = ColumnInformation.create("insert_id", ColumnType.BIGINT);
        TEXT_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .toFormatter();

        TEXT_OFFSET_DATE_TIME = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(TEXT_LOCAL_DATE_TIME)
                .appendOffsetId()
                .toFormatter();

        TEXT_ZONED_DATE_TIME = new DateTimeFormatterBuilder()
                .append(TEXT_OFFSET_DATE_TIME)
                .optionalStart()
                .appendLiteral('[')
                .parseCaseSensitive()
                .appendZoneRegionId()
                .appendLiteral(']')
                .toFormatter();
    }

    protected boolean isBinaryEncoded;
    protected TimeZone timeZone;
    protected Options options;
    private Protocol protocol;
    private PacketInputStream reader;
    protected ColumnInformation[] columnsInformation;
    private boolean isEof;
    protected int columnInformationLength;
    protected boolean noBackslashEscapes;
    private boolean callableResult;
    private MariaDbStatement statement;
    private RowProtocol row;
    private int dataFetchTime;
    private boolean streaming;
    private byte[][] data;
    private int dataSize;
    private int fetchSize;
    private int resultSetScrollType;
    private int rowPointer;
    private ColumnNameMap columnNameMap;
    private int lastValueNull;
    private int lastRowPointer = -1;
    private int dataTypeMappingFlags;
    private boolean returnTableAlias;
    private boolean isClosed;
    private boolean eofDeprecated;


    /**
     * Create Streaming resultSet.
     *
     * @param columnInformation column information
     * @param results           results
     * @param protocol          current protocol
     * @param reader            stream fetcher
     * @param callableResult    is it from a callableStatement ?
     * @param eofDeprecated     is EOF deprecated
     * @throws IOException  if any connection error occur
     * @throws SQLException if any connection error occur
     */
    public SelectResultSet(ColumnInformation[] columnInformation, Results results, Protocol protocol,
                           PacketInputStream reader, boolean callableResult, boolean eofDeprecated)
            throws IOException, SQLException {
        this.statement = results.getStatement();
        this.isClosed = false;
        this.protocol = protocol;
        this.options = protocol.getOptions();
        this.noBackslashEscapes = protocol.noBackslashEscapes();
        this.timeZone = protocol.getTimeZone();
        this.dataTypeMappingFlags = protocol.getDataTypeMappingFlags();
        this.returnTableAlias = this.options.useOldAliasMetadataBehavior;
        this.columnsInformation = columnInformation;
        this.columnNameMap = new ColumnNameMap(columnsInformation);

        this.columnInformationLength = columnInformation.length;
        this.reader = reader;
        this.isEof = false;
        this.isBinaryEncoded = results.isBinaryFormat();
        if (isBinaryEncoded) {
            row = new BinaryRowProtocol(columnsInformation, columnInformationLength, results.getMaxFieldSize());
        } else {
            row = new TextRowProtocol(results.getMaxFieldSize());
        }
        this.fetchSize = results.getFetchSize();
        this.resultSetScrollType = results.getResultSetScrollType();
        this.dataSize = 0;
        this.dataFetchTime = 0;
        this.rowPointer = -1;
        this.callableResult = callableResult;
        this.eofDeprecated = eofDeprecated;

        if (fetchSize == 0 || callableResult) {
            this.data = new byte[10][];
            fetchAllResults();
            streaming = false;
        } else {
            protocol.setActiveStreamingResult(results);
            protocol.removeHasMoreResults();
            data = new byte[Math.max(10, fetchSize)][];
            nextStreamingValue();
            streaming = true;
        }

    }

    /**
     * Create filled result-set.
     *
     * @param columnInformation   column information
     * @param resultSet           result-set data
     * @param protocol            current protocol
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     */
    public SelectResultSet(ColumnInformation[] columnInformation, List<byte[]> resultSet, Protocol protocol,
                           int resultSetScrollType) {
        this.statement = null;
        this.isClosed = false;
        this.row = new TextRowProtocol(0);
        if (protocol != null) {
            this.options = protocol.getOptions();
            this.timeZone = protocol.getTimeZone();
            this.dataTypeMappingFlags = protocol.getDataTypeMappingFlags();
            this.returnTableAlias = this.options.useOldAliasMetadataBehavior;
        } else {
            this.options = null;
            this.timeZone = TimeZone.getDefault();
            this.dataTypeMappingFlags = 3;
            this.returnTableAlias = false;
        }
        this.protocol = null;
        this.columnsInformation = columnInformation;
        this.columnNameMap = new ColumnNameMap(columnsInformation);
        this.columnInformationLength = columnInformation.length;
        this.isEof = true;
        this.isBinaryEncoded = false;
        this.fetchSize = 1;
        this.resultSetScrollType = resultSetScrollType;
        this.data = resultSet.toArray(new byte[10][]);
        this.dataSize = resultSet.size();
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
        columns[0] = ColumnInformation.create("insert_id", ColumnType.BIGINT);

        List<byte[]> rows = new ArrayList<>();
        for (long rowData : data) {
            if (rowData != 0) {
                rows.add(StandardPacketInputStream.create(String.valueOf(rowData).getBytes()));
            }
        }
        if (findColumnReturnsOne) {
            return new SelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE) {
                @Override
                public int findColumn(String name) {
                    return 1;
                }
            };
        }
        return new SelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
    }

    /**
     * Create a result set from given data. Useful for creating "fake" resultSets for DatabaseMetaData, (one example is
     * MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param columnNames - string array of column names
     * @param columnTypes - column types
     * @param data        - each element of this array represents a complete row in the ResultSet. Each value is given in its string representation,
     *                    as in MySQL text protocol, except boolean (BIT(1)) values that are represented as "1" or "0" strings
     * @param protocol    protocol
     * @return resultset
     */
    public static ResultSet createResultSet(String[] columnNames, ColumnType[] columnTypes, String[][] data,
                                            Protocol protocol) {
        int columnNameLength = columnNames.length;
        ColumnInformation[] columns = new ColumnInformation[columnNameLength];

        for (int i = 0; i < columnNameLength; i++) {
            columns[i] = ColumnInformation.create(columnNames[i], columnTypes[i]);
        }

        List<byte[]> rows = new ArrayList<>();

        for (String[] rowData : data) {
            assert rowData.length == columnNameLength;
            byte[][] rowBytes = new byte[rowData.length][];
            for (int i = 0; i < rowData.length; i++) {
                if (rowData[i] != null) rowBytes[i] = rowData[i].getBytes();
            }
            rows.add(StandardPacketInputStream.create(rowBytes, columnTypes));
        }
        return new SelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
    }

    public static SelectResultSet createEmptyResultSet() {
        return new SelectResultSet(INSERT_ID_COLUMNS, new ArrayList<>(), null,
                TYPE_SCROLL_SENSITIVE);
    }

    private void fetchAllResults() throws IOException, SQLException {

        dataSize = 0;
        while (readNextValue()) {
            //fetch all results
        }
        dataFetchTime++;
    }

    /**
     * When protocol has a current Streaming result (this) fetch all to permit another query is executing.
     *
     * @throws SQLException if any error occur
     */
    public void fetchRemaining() throws SQLException {
        try {
            try {
                if (!isEof) {
                    lastRowPointer = -1;
                    ReentrantLock lock = protocol.getLock();
                    lock.lock();
                    try {
                        while (!isEof) {
                            addStreamingValue();
                        }
                    } finally {
                        lock.unlock();
                    }
                }

            } catch (IOException ioexception) {
                throw new SQLException("Could not close resultSet : " + ioexception.getMessage(),
                        CONNECTION_EXCEPTION.getSqlState(), ioexception);
            }
        } catch (SQLException queryException) {
            ExceptionMapper.throwException(queryException, null, this.statement);
        }

        dataFetchTime++;
    }

    private void fetchRemainingLock() throws SQLException {
        if (!isEof) {
            //load remaining results
            ReentrantLock lock = protocol.getLock();
            lock.lock();
            try {
                fetchRemaining();
            } catch (SQLException ioe) {
                throw new SQLException("Server has closed the connection. If result set contain huge amount of data, Server expects client to"
                        + " read off the result set relatively fast. "
                        + "In this case, please consider increasing net_wait_timeout session variable."
                        + " / processing your result set faster (check Streaming result sets documentation for more information)", ioe);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * This permit to replace current stream results by next ones.
     *
     * @throws IOException  if socket exception occur
     * @throws SQLException if server return an unexpected error
     */
    private void nextStreamingValue() throws IOException, SQLException {
        lastRowPointer = -1;

        //if resultSet can be back to some previous value
        if (resultSetScrollType == TYPE_FORWARD_ONLY) dataSize = 0;

        addStreamingValue();

    }

    /**
     * This permit to add next streaming values to existing resultSet.
     *
     * @throws IOException  if socket exception occur
     * @throws SQLException if server return an unexpected error
     */
    private void addStreamingValue() throws IOException, SQLException {
        //read only fetchSize values
        int fetchSizeTmp = fetchSize;
        while (fetchSizeTmp > 0 && readNextValue()) {
            fetchSizeTmp--;
        }
        dataFetchTime++;

    }

    /**
     * Read next value.
     *
     * @return true if have a new value
     * @throws IOException  exception
     * @throws SQLException exception
     */
    private boolean readNextValue() throws IOException, SQLException {
        byte[] buf = reader.getPacketArray(false);

        //is error Packet
        if (buf[0] == ERROR) {
            protocol.removeActiveStreamingResult();
            protocol.removeHasMoreResults();
            protocol.setHasWarnings(false);
            ErrorPacket errorPacket = new ErrorPacket(new Buffer(buf));
            resetVariables();
            if (statement != null) {
                throw new SQLException("(conn:" + statement.getServerThreadId() + ") " + errorPacket.getMessage(),
                        errorPacket.getSqlState(), errorPacket.getErrorNumber());
            } else {
                throw new SQLException(errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorNumber());
            }
        }

        //is end of stream
        if (buf[0] == EOF && ((eofDeprecated && buf.length < 0xffffff)
                || (!eofDeprecated && buf.length < 8))) {
            int serverStatus;
            int warnings;

            if (!eofDeprecated) {
                //EOF_Packet
                warnings = (buf[1] & 0xff) + ((buf[2] & 0xff) << 8);
                serverStatus = ((buf[3] & 0xff) + ((buf[4] & 0xff) << 8));

                //CallableResult has been read from intermediate EOF server_status
                //and is mandatory because :
                //
                // - Call query will have an callable resultSet for OUT parameters
                //   this resultSet must be identified and not listed in JDBC statement.getResultSet()
                //
                // - after a callable resultSet, a OK packet is send,
                //   but mysql before 5.7.4 doesn't send MORE_RESULTS_EXISTS flag
                if (callableResult) serverStatus |= MORE_RESULTS_EXISTS;

            } else {

                //OK_Packet with a 0xFE header
                int pos = skipLengthEncodedValue(buf, 1); //skip update count
                pos = skipLengthEncodedValue(buf, pos); //skip insert id
                serverStatus = ((buf[pos++] & 0xff) + ((buf[pos++] & 0xff) << 8));
                warnings = (buf[pos++] & 0xff) + ((buf[pos] & 0xff) << 8);
                callableResult = (serverStatus & PS_OUT_PARAMETERS) != 0;
            }
            protocol.setServerStatus((short) serverStatus);
            protocol.setHasWarnings(warnings > 0);
            if ((serverStatus & MORE_RESULTS_EXISTS) == 0) protocol.removeActiveStreamingResult();

            resetVariables();
            return false;
        }

        //this is a result-set row, save it
        if (dataSize + 1 >= data.length) growDataArray();
        data[dataSize++] = buf;
        return true;
    }

    /**
     * Get current row's raw bytes.
     * @return row's raw bytes
     */
    protected byte[] getCurrentRowData() {
        return data[rowPointer];
    }

    /**
     * Update row's raw bytes.
     * in case of row update, refresh the data.
     * (format must correspond to current resultset binary/text row encryption)
     *
     * @param rawData new row's raw data.
     */
    protected void updateRowData(byte[] rawData) {
        data[rowPointer] = rawData;
        row.resetRow(data[rowPointer]);
    }

    /**
     * Delete current data.
     * Position cursor to the previous row.
     * @throws SQLException if previous() fail.
     */
    protected void deleteCurrentRowData() throws SQLException {
        //move data
        System.arraycopy(data, rowPointer + 1, data, rowPointer, dataSize - 1 - rowPointer);
        data[dataSize - 1] = null;
        dataSize--;
        lastRowPointer = -1;
        previous();
    }

    protected void addRowData(byte[] rawData) {
        if (dataSize + 1 >= data.length) growDataArray();
        data[dataSize] = rawData;
        rowPointer = dataSize;
        dataSize++;
    }

    private int skipLengthEncodedValue(byte[] buf, int pos) {
        int type = buf[pos++] & 0xff;
        switch (type) {
            case 251:
                return pos;
            case 252:
                return pos + 2 + (0xffff & (((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8))));
            case 253:
                return pos + 3 + (0xffffff & ((buf[pos] & 0xff)
                        + ((buf[pos + 1] & 0xff) << 8)
                        + ((buf[pos + 2] & 0xff) << 16)));
            case 254:
                return (int) (pos + 8 + ((buf[pos] & 0xff)
                        + ((long) (buf[pos + 1] & 0xff) << 8)
                        + ((long) (buf[pos + 2] & 0xff) << 16)
                        + ((long) (buf[pos + 3] & 0xff) << 24)
                        + ((long) (buf[pos + 4] & 0xff) << 32)
                        + ((long) (buf[pos + 5] & 0xff) << 40)
                        + ((long) (buf[pos + 6] & 0xff) << 48)
                        + ((long) (buf[pos + 7] & 0xff) << 56)));
            default:
                return pos + type;
        }
    }

    /**
     * Grow data array.
     */
    private void growDataArray() {
        int newCapacity = data.length + (data.length >> 1);
        if (newCapacity - MAX_ARRAY_SIZE > 0) newCapacity = MAX_ARRAY_SIZE;
        data = Arrays.copyOf(data, newCapacity);
    }

    /**
     * Close resultSet.
     */
    public void close() throws SQLException {
        isClosed = true;
        if (protocol != null) {
            ReentrantLock lock = protocol.getLock();
            lock.lock();
            try {
                while (!isEof) {
                    dataSize = 0; //to avoid storing data
                    readNextValue();
                }

            } catch (IOException ioexception) {
                ExceptionMapper.throwException(new SQLException(
                        "Could not close resultSet : " + ioexception.getMessage() + protocol.getTraces(),
                        CONNECTION_EXCEPTION.getSqlState(), ioexception), null, this.statement);
            } catch (SQLException queryException) {
                ExceptionMapper.throwException(queryException, null, this.statement);
            } finally {
                resetVariables();
                lock.unlock();
            }
        } else {
            resetVariables();
        }

        //keep garbage easy
        for (int i = 0; i < data.length; i++) data[i] = null;

        if (statement != null) {
            statement.checkCloseOnCompletion(this);
            statement = null;
        }
    }

    private void resetVariables() {
        protocol = null;
        reader = null;
        isEof = true;
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed) throw new SQLException("Operation not permit on a closed resultSet", "HY000");
        if (rowPointer < dataSize - 1) {
            rowPointer++;
            return true;
        } else {
            if (streaming && !isEof) {
                ReentrantLock lock = protocol.getLock();
                lock.lock();
                try {
                    nextStreamingValue();
                } catch (IOException ioe) {
                    throw new SQLException("Server has closed the connection. If result set contain huge amount of data, Server expects client to"
                            + " read off the result set relatively fast. "
                            + "In this case, please consider increasing net_wait_timeout session variable."
                            + " / processing your result set faster (check Streaming result sets documentation for more information)", ioe);
                } finally {
                    lock.unlock();
                }

                if (resultSetScrollType == TYPE_FORWARD_ONLY) {
                    //resultSet has been cleared. next value is pointer 0.
                    rowPointer = 0;
                    return dataSize > 0;
                } else {
                    // cursor can move backward, so driver must keep the results.
                    // results have been added to current resultSet
                    rowPointer++;
                    return dataSize > rowPointer;
                }
            }

            //all data are reads and pointer is after last
            rowPointer = dataSize;
            return false;
        }
    }

    private void checkObjectRange(int position) throws SQLException {
        if (rowPointer < 0) {
            throw new SQLDataException("Current position is before the first row", "22023");
        }

        if (rowPointer >= dataSize) {
            throw new SQLDataException("Current position is after the last row", "22023");
        }

        if (position <= 0 || position > columnInformationLength) {
            throw new SQLDataException("No such column: " + position, "22023");
        }

        if (lastRowPointer != rowPointer) {
            row.resetRow(data[rowPointer]);
            lastRowPointer = rowPointer;
        }
        this.lastValueNull = row.setPosition(position - 1) ? BIT_LAST_FIELD_NULL : BIT_LAST_FIELD_NOT_NULL;
    }

    private boolean lastValueWasNull() {
        return (lastValueNull & BIT_LAST_FIELD_NULL) != 0;
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
        return (dataFetchTime > 0) ? rowPointer == -1 && dataSize > 0 : rowPointer == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClose();
        if (rowPointer < dataSize) {

            //has remaining results
            return false;

        } else {

            if (streaming && !isEof) {

                //has to read more result to know if it's finished or not
                //(next packet may be new data or an EOF packet indicating that there is no more data)
                ReentrantLock lock = protocol.getLock();
                lock.lock();
                try {
                    //this time, fetch is added even for streaming forward type only to keep current pointer row.
                    addStreamingValue();
                } catch (IOException ioe) {
                    throw new SQLException("Server has closed the connection. If result set contain huge amount of data, Server expects client to"
                            + " read off the result set relatively fast. "
                            + "In this case, please consider increasing net_wait_timeout session variable."
                            + " / processing your result set faster (check Streaming result sets documentation for more information)", ioe);
                } finally {
                    lock.unlock();
                }

                return dataSize == rowPointer;
            }

            //has read all data and pointer is after last result
            //so result would have to always to be true,
            //but when result contain no row at all jdbc say that must return false
            return dataSize > 0 || dataFetchTime > 1;
        }
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClose();
        return dataFetchTime == 1 && rowPointer == 0 && dataSize > 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClose();
        if (rowPointer < dataSize - 1) {
            return false;
        } else if (isEof) {
            return rowPointer == dataSize - 1 && dataSize > 0;
        } else {
            //when streaming and not having read all results,
            //must read next packet to know if next packet is an EOF packet or some additional data
            ReentrantLock lock = protocol.getLock();
            lock.lock();
            try {
                addStreamingValue();
            } catch (IOException ioe) {
                throw new SQLException("Server has closed the connection. If result set contain huge amount of data, Server expects client to"
                        + " read off the result set relatively fast. "
                        + "In this case, please consider increasing net_wait_timeout session variable."
                        + " / processing your result set faster (check Streaming result sets documentation for more information)", ioe);
            } finally {
                lock.unlock();
            }

            if (isEof) {
                //now driver is sure when data ends.
                return rowPointer == dataSize - 1 && dataSize > 0;
            }

            //There is data remaining
            return false;
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClose();

        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        }
        rowPointer = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClose();
        fetchRemainingLock();
        rowPointer = dataSize;
    }

    @Override
    public boolean first() throws SQLException {
        checkClose();

        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        }

        rowPointer = 0;
        return dataSize > 0;
    }

    @Override
    public boolean last() throws SQLException {
        checkClose();
        fetchRemainingLock();
        rowPointer = dataSize - 1;
        return rowPointer > 0;
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
        }

        if (row >= 0 && row <= dataSize) {
            rowPointer = row - 1;
            return true;
        }

        //if streaming, must read additional results.
        fetchRemainingLock();

        if (row >= 0) {

            if (row <= dataSize) {
                rowPointer = row - 1;
                return true;
            }

            rowPointer = dataSize; //go to afterLast() position
            return false;

        } else {

            if (dataSize + row >= 0) {
                //absolute position reverse from ending resultSet
                rowPointer = dataSize + row;
                return true;
            }

            rowPointer = -1; // go to before first position
            return false;

        }

    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        }
        int newPos = rowPointer + rows;
        if (newPos > -1 && newPos <= dataSize) {
            rowPointer = newPos;
            return true;
        }
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        checkClose();
        if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        }
        if (rowPointer > -1) {
            rowPointer--;
            return rowPointer != -1;
        }
        return false;
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
        if (streaming && fetchSize == 0) {

            try {

                while (!isEof) {
                    //fetch all results
                    addStreamingValue();
                }
            } catch (IOException ioException) {
                throw new SQLException(ioException);
            }

            streaming = dataFetchTime == 1;
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
            throw new SQLException("Operation not permit on a closed resultSet", "HY000");
        }
    }

    public boolean isCallableResult() {
        return callableResult;
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
        return (lastValueNull & BIT_LAST_FIELD_NULL) != 0
                || (lastValueNull & BIT_LAST_ZERO_DATE) != 0;
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
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        return new ByteArrayInputStream(new String(row.buf, row.pos, row.getLengthMaxFieldSize(), StandardCharsets.UTF_8).getBytes());
    }

    /**
     * {inheritDoc}.
     */
    public String getString(int columnIndex) throws SQLException {
        checkObjectRange(columnIndex);
        return getInternalString(columnsInformation[columnIndex - 1], null);
    }

    /**
     * {inheritDoc}.
     */
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    private String getInternalString(ColumnInformation columnInfo) throws SQLException {
        return getInternalString(columnInfo, null);
    }

    private String getInternalString(ColumnInformation columnInfo, Calendar cal) throws SQLException {
        if (lastValueWasNull()) return null;

        switch (columnInfo.getColumnType()) {
            case STRING:
                if (row.getMaxFieldSize() > 0) {
                    return new String(row.buf, row.pos, Math.max(row.getMaxFieldSize() * 3, row.length), StandardCharsets.UTF_8)
                            .substring(0, row.getMaxFieldSize());
                }
                return new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);

            case BIT:
                if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
                    return (row.buf[row.pos] == 0) ? "0" : "1";
                }
                break;
            case TINYINT:
                if (this.isBinaryEncoded) {
                    return zeroFillingIfNeeded(String.valueOf(getInternalTinyInt(columnInfo)), columnInfo);
                }
                break;
            case SMALLINT:
                if (this.isBinaryEncoded) {
                    return zeroFillingIfNeeded(String.valueOf(getInternalSmallInt(columnInfo)), columnInfo);
                }
                break;
            case INTEGER:
            case MEDIUMINT:
                if (this.isBinaryEncoded) {
                    return zeroFillingIfNeeded(String.valueOf(getInternalMediumInt(columnInfo)), columnInfo);
                }
                break;
            case BIGINT:
                if (this.isBinaryEncoded) {
                    if (!columnInfo.isSigned()) {
                        return zeroFillingIfNeeded(String.valueOf(getInternalBigInteger(columnInfo)), columnInfo);
                    }
                    return zeroFillingIfNeeded(String.valueOf(getInternalLong(columnInfo)), columnInfo);
                }
                break;
            case DOUBLE:
                return zeroFillingIfNeeded(String.valueOf(getInternalDouble(columnInfo)), columnInfo);
            case FLOAT:
                return zeroFillingIfNeeded(String.valueOf(getInternalFloat(columnInfo)), columnInfo);
            case TIME:
                return getTimeString(columnInfo);
            case DATE:
                if (isBinaryEncoded) {
                    Date date = getInternalDate(columnInfo, cal);
                    if (date == null) {
                        //specific for "zero-date", getString will return "zero-date" value -> wasNull() must then return false
                        lastValueNull ^= BIT_LAST_ZERO_DATE;
                        return new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                    }
                    return date.toString();
                }
                break;
            case YEAR:
                if (options.yearIsDateType) {
                    Date date = getInternalDate(columnInfo, cal);
                    return (date == null) ? null : date.toString();
                }
                if (this.isBinaryEncoded) {
                    return String.valueOf(getInternalSmallInt(columnInfo));
                }
                break;
            case TIMESTAMP:
            case DATETIME:
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal);
                if (timestamp == null) {
                    //specific for "zero-date", getString will return "zero-date" value -> wasNull() must then return false
                    lastValueNull ^= BIT_LAST_ZERO_DATE;
                    return new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                }
                return timestamp.toString();
            case DECIMAL:
            case OLDDECIMAL:
                BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
                return (bigDecimal == null) ? null : zeroFillingIfNeeded(bigDecimal.toString(), columnInfo);
            case GEOMETRY:
                return new String(row.buf, row.pos, row.length);
            case NULL:
                return null;
            default:
                if (row.getMaxFieldSize() > 0) {
                    return new String(row.buf, row.pos, Math.max(row.getMaxFieldSize() * 3, row.length), StandardCharsets.UTF_8)
                            .substring(0, row.getMaxFieldSize());
                }
                return new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
        }

        return new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
    }

    private String zeroFillingIfNeeded(String value, ColumnInformation columnInformation) {
        if (columnInformation.isZeroFill()) {
            StringBuilder zeroAppendStr = new StringBuilder();
            long zeroToAdd = columnInformation.getDisplaySize() - value.length();
            while (zeroToAdd-- > 0) zeroAppendStr.append("0");
            return zeroAppendStr.append(value).toString();
        }
        return value;
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        return new ByteArrayInputStream(row.buf, row.pos, row.getLengthMaxFieldSize());
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
        checkObjectRange(columnIndex);
        return getInternalInt(columnsInformation[columnIndex - 1]);
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
     * @param columnInfo current column information
     * @return int
     */
    private int getInternalInt(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;

        if (!this.isBinaryEncoded) {
            return parseInt(columnInfo);
        } else {
            long value;
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                    value = getInternalTinyInt(columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getInternalSmallInt(columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = ((row.buf[row.pos] & 0xff)
                            + ((row.buf[row.pos + 1] & 0xff) << 8)
                            + ((row.buf[row.pos + 2] & 0xff) << 16)
                            + ((row.buf[row.pos + 3] & 0xff) << 24));
                    if (columnInfo.isSigned()) {
                        return (int) value;
                    } else if (value < 0) {
                        value = value & 0xffffffffL;
                    }
                    break;
                case BIGINT:
                    value = getInternalLong(columnInfo);
                    break;
                case FLOAT:
                    value = (long) getInternalFloat(columnInfo);
                    break;
                case DOUBLE:
                    value = (long) getInternalDouble(columnInfo);
                    break;
                default:
                    return parseInt(columnInfo);
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
        checkObjectRange(columnIndex);
        return getInternalLong(columnsInformation[columnIndex - 1]);
    }

    /**
     * Get long from raw data.
     *
     * @param columnInfo current column information
     * @return long
     * @throws SQLException if any error occur
     */
    private long getInternalLong(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;

        if (!this.isBinaryEncoded) {
            return parseLong(columnInfo);
        } else {
            long value;
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                    value = getInternalTinyInt(columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getInternalSmallInt(columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getInternalMediumInt(columnInfo);
                    break;
                case BIGINT:
                    value = ((row.buf[row.pos] & 0xff)
                            + ((long) (row.buf[row.pos + 1] & 0xff) << 8)
                            + ((long) (row.buf[row.pos + 2] & 0xff) << 16)
                            + ((long) (row.buf[row.pos + 3] & 0xff) << 24)
                            + ((long) (row.buf[row.pos + 4] & 0xff) << 32)
                            + ((long) (row.buf[row.pos + 5] & 0xff) << 40)
                            + ((long) (row.buf[row.pos + 6] & 0xff) << 48)
                            + ((long) (row.buf[row.pos + 7] & 0xff) << 56));
                    if (columnInfo.isSigned()) {
                        return value;
                    }
                    BigInteger unsignedValue = new BigInteger(1, new byte[]{(byte) (value >> 56),
                            (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                            (byte) value});
                    if (unsignedValue.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + unsignedValue + " is not in Long range", "22003", 1264);
                    }
                    return unsignedValue.longValue();
                case FLOAT:
                    Float floatValue = getInternalFloat(columnInfo);
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + floatValue
                                + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = getInternalDouble(columnInfo);
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + doubleValue
                                + " is not in Long range", "22003", 1264);
                    }
                    return doubleValue.longValue();
                default:
                    return parseLong(columnInfo);
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
        checkObjectRange(columnIndex);
        return getInternalFloat(columnsInformation[columnIndex - 1]);
    }

    /**
     * Get float from raw data.
     *
     * @param columnInfo current column information
     * @return float
     * @throws SQLException id any error occur
     */
    @SuppressWarnings("UnnecessaryInitCause")
    private float getInternalFloat(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;

        if (!this.isBinaryEncoded) {
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                case VARSTRING:
                case VARCHAR:
                case STRING:
                case OLDDECIMAL:
                case BIGINT:
                    try {
                        return Float.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        SQLException sqlException = new SQLException("Incorrect format \""
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + "\" for getFloat for data field with type " + columnInfo.getColumnType().getJavaTypeName(), "22003", 1264);
                        //noinspection UnnecessaryInitCause
                        sqlException.initCause(nfe);
                        throw sqlException;
                    }
                default:
                    throw new SQLException("getFloat not available for data field type " + columnInfo.getColumnType().getJavaTypeName());
            }
        } else {
            long value;
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                    value = getInternalTinyInt(columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getInternalSmallInt(columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getInternalMediumInt(columnInfo);
                    break;
                case BIGINT:
                    value = ((row.buf[row.pos] & 0xff)
                            + ((long) (row.buf[row.pos + 1] & 0xff) << 8)
                            + ((long) (row.buf[row.pos + 2] & 0xff) << 16)
                            + ((long) (row.buf[row.pos + 3] & 0xff) << 24)
                            + ((long) (row.buf[row.pos + 4] & 0xff) << 32)
                            + ((long) (row.buf[row.pos + 5] & 0xff) << 40)
                            + ((long) (row.buf[row.pos + 6] & 0xff) << 48)
                            + ((long) (row.buf[row.pos + 7] & 0xff) << 56));
                    if (columnInfo.isSigned()) {
                        return value;
                    }
                    BigInteger unsignedValue = new BigInteger(1, new byte[]{(byte) (value >> 56),
                            (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                            (byte) value});
                    return unsignedValue.floatValue();
                case FLOAT:
                    int valueFloat = ((row.buf[row.pos] & 0xff)
                            + ((row.buf[row.pos + 1] & 0xff) << 8)
                            + ((row.buf[row.pos + 2] & 0xff) << 16)
                            + ((row.buf[row.pos + 3] & 0xff) << 24));
                    return Float.intBitsToFloat(valueFloat);
                case DOUBLE:
                    return (float) getInternalDouble(columnInfo);
                case DECIMAL:
                case VARSTRING:
                case VARCHAR:
                case STRING:
                case OLDDECIMAL:
                    try {
                        return Float.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        SQLException sqlException = new SQLException("Incorrect format for getFloat for data field with type "
                                + columnInfo.getColumnType().getJavaTypeName(), "22003", 1264);
                        sqlException.initCause(nfe);
                        throw sqlException;
                    }
                default:
                    throw new SQLException("getFloat not available for data field type " + columnInfo.getColumnType().getJavaTypeName());
            }
            try {
                return Float.valueOf(String.valueOf(value));
            } catch (NumberFormatException nfe) {
                SQLException sqlException = new SQLException("Incorrect format for getFloat for data field with type "
                        + columnInfo.getColumnType().getJavaTypeName(), "22003", 1264);
                sqlException.initCause(nfe);
                throw sqlException;
            }
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
        checkObjectRange(columnIndex);
        return getInternalDouble(columnsInformation[columnIndex - 1]);
    }


    /**
     * Get double value from raw data.
     *
     * @param columnInfo current column information
     * @return double
     * @throws SQLException id any error occur
     */
    private double getInternalDouble(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        if (!this.isBinaryEncoded) {
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                case VARSTRING:
                case VARCHAR:
                case STRING:
                case OLDDECIMAL:
                case BIGINT:
                    try {
                        return Double.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        SQLException sqlException = new SQLException("Incorrect format \""
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + "\" for getDouble for data field with type " + columnInfo.getColumnType().getJavaTypeName(), "22003", 1264);
                        //noinspection UnnecessaryInitCause
                        sqlException.initCause(nfe);
                        throw sqlException;
                    }
                default:
                    throw new SQLException("getDouble not available for data field type " + columnInfo.getColumnType().getJavaTypeName());
            }
        } else {
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                    return getInternalTinyInt(columnInfo);
                case SMALLINT:
                case YEAR:
                    return getInternalSmallInt(columnInfo);
                case INTEGER:
                case MEDIUMINT:
                    return getInternalMediumInt(columnInfo);
                case BIGINT:
                    long valueLong = ((row.buf[row.pos] & 0xff)
                            + ((long) (row.buf[row.pos + 1] & 0xff) << 8)
                            + ((long) (row.buf[row.pos + 2] & 0xff) << 16)
                            + ((long) (row.buf[row.pos + 3] & 0xff) << 24)
                            + ((long) (row.buf[row.pos + 4] & 0xff) << 32)
                            + ((long) (row.buf[row.pos + 5] & 0xff) << 40)
                            + ((long) (row.buf[row.pos + 6] & 0xff) << 48)
                            + ((long) (row.buf[row.pos + 7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return valueLong;
                    } else {
                        return new BigInteger(1, new byte[]{(byte) (valueLong >> 56),
                                (byte) (valueLong >> 48), (byte) (valueLong >> 40), (byte) (valueLong >> 32),
                                (byte) (valueLong >> 24), (byte) (valueLong >> 16), (byte) (valueLong >> 8),
                                (byte) valueLong}).doubleValue();
                    }
                case FLOAT:
                    return getInternalFloat(columnInfo);
                case DOUBLE:
                    long valueDouble = ((row.buf[row.pos] & 0xff)
                            + ((long) (row.buf[row.pos + 1] & 0xff) << 8)
                            + ((long) (row.buf[row.pos + 2] & 0xff) << 16)
                            + ((long) (row.buf[row.pos + 3] & 0xff) << 24)
                            + ((long) (row.buf[row.pos + 4] & 0xff) << 32)
                            + ((long) (row.buf[row.pos + 5] & 0xff) << 40)
                            + ((long) (row.buf[row.pos + 6] & 0xff) << 48)
                            + ((long) (row.buf[row.pos + 7] & 0xff) << 56));
                    return Double.longBitsToDouble(valueDouble);
                case DECIMAL:
                case VARSTRING:
                case VARCHAR:
                case STRING:
                case OLDDECIMAL:
                    try {
                        return Double.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        SQLException sqlException = new SQLException("Incorrect format for getDouble for data field with type "
                                + columnInfo.getColumnType().getJavaTypeName(), "22003", 1264);
                        //noinspection UnnecessaryInitCause
                        sqlException.initCause(nfe);
                        throw sqlException;
                    }
                default:
                    throw new SQLException("getDouble not available for data field type "
                            + columnInfo.getColumnType().getJavaTypeName());
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
        checkObjectRange(columnIndex);
        return getInternalBigDecimal(columnsInformation[columnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkObjectRange(columnIndex);
        return getInternalBigDecimal(columnsInformation[columnIndex - 1]);
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
     * @param columnInfo current column information
     * @return Bigdecimal value
     * @throws SQLException id any error occur
     */
    private BigDecimal getInternalBigDecimal(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return null;

        if (!this.isBinaryEncoded) {
            return new BigDecimal(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
        } else {
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return BigDecimal.valueOf((long) row.buf[row.pos]);
                case TINYINT:
                    return BigDecimal.valueOf((long) getInternalTinyInt(columnInfo));
                case SMALLINT:
                case YEAR:
                    return BigDecimal.valueOf((long) getInternalSmallInt(columnInfo));
                case INTEGER:
                case MEDIUMINT:
                    return BigDecimal.valueOf(getInternalMediumInt(columnInfo));
                case BIGINT:
                    long value = ((row.buf[row.pos] & 0xff)
                            + ((long) (row.buf[row.pos + 1] & 0xff) << 8)
                            + ((long) (row.buf[row.pos + 2] & 0xff) << 16)
                            + ((long) (row.buf[row.pos + 3] & 0xff) << 24)
                            + ((long) (row.buf[row.pos + 4] & 0xff) << 32)
                            + ((long) (row.buf[row.pos + 5] & 0xff) << 40)
                            + ((long) (row.buf[row.pos + 6] & 0xff) << 48)
                            + ((long) (row.buf[row.pos + 7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return new BigDecimal(String.valueOf(BigInteger.valueOf(value))).setScale(columnInfo.getDecimals());
                    } else {
                        return new BigDecimal(String.valueOf(new BigInteger(1, new byte[]{(byte) (value >> 56),
                                (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                                (byte) value}))).setScale(columnInfo.getDecimals());
                    }
                case FLOAT:
                    return BigDecimal.valueOf(getInternalFloat(columnInfo));
                case DOUBLE:
                    return BigDecimal.valueOf(getInternalDouble(columnInfo));
                default:
                    return new BigDecimal(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
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
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        byte[] data = new byte[row.getLengthMaxFieldSize()];
        System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
        return data;
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(int columnIndex) throws SQLException {
        checkObjectRange(columnIndex);
        return getInternalDate(columnsInformation[columnIndex - 1], null);
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
        checkObjectRange(columnIndex);
        return getInternalDate(columnsInformation[columnIndex - 1], cal);
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
     * @param columnInfo current column information
     * @param cal        session calendar
     * @return date
     * @throws SQLException if raw data cannot be parse
     */
    private Date getInternalDate(ColumnInformation columnInfo, Calendar cal) throws SQLException {
        if (lastValueWasNull()) return null;

        if (!this.isBinaryEncoded) {
            String rawValue = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
            switch (columnInfo.getColumnType()) {
                case TIMESTAMP:
                case DATETIME:
                    Timestamp timestamp = getInternalTimestamp(columnInfo, cal);
                    if (timestamp == null) return null;
                    return new Date(timestamp.getTime());

                case TIME:
                    throw new SQLException("Cannot read DATE using a Types.TIME field");

                case DATE:
                    if ("0000-00-00".equals(rawValue)) {
                        lastValueNull |= BIT_LAST_ZERO_DATE;
                        return null;
                    }

                    return new Date(
                            Integer.parseInt(rawValue.substring(0, 4)) - 1900,
                            Integer.parseInt(rawValue.substring(5, 7)) - 1,
                            Integer.parseInt(rawValue.substring(8, 10))
                    );

                case YEAR:
                    int year = Integer.parseInt(rawValue);
                    if (row.length == 2 && columnInfo.getLength() == 2) {
                        if (year <= 69) {
                            year += 2000;
                        } else {
                            year += 1900;
                        }
                    }

                    return new Date(year - 1900, 0, 1);

                default:

                    try {

                        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        sdf.setTimeZone(timeZone);
                        java.util.Date utilDate = sdf.parse(rawValue);
                        return new Date(utilDate.getTime());

                    } catch (ParseException e) {
                        throw ExceptionMapper.getSqlException("Could not get object as Date : " + e.getMessage(), "S1009", e);
                    }
            }

        } else {
            return binaryDate(columnInfo, cal);
        }
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(int columnIndex) throws SQLException {
        checkObjectRange(columnIndex);
        return getInternalTime(columnsInformation[columnIndex - 1], null);
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
        checkObjectRange(columnIndex);
        return getInternalTime(columnsInformation[columnIndex - 1], cal);
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
     * @param columnInfo current column information
     * @param cal        session calendar
     * @return time value
     * @throws SQLException if raw data cannot be parse
     */
    private Time getInternalTime(ColumnInformation columnInfo, Calendar cal) throws SQLException {
        if (lastValueWasNull()) return null;

        if (!this.isBinaryEncoded) {
            if (columnInfo.getColumnType() == ColumnType.TIMESTAMP || columnInfo.getColumnType() == ColumnType.DATETIME) {
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal);
                return (timestamp == null) ? null : new Time(timestamp.getTime());

            } else if (columnInfo.getColumnType() == ColumnType.DATE) {

                throw new SQLException("Cannot read Time using a Types.DATE field");

            } else {
                String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                if (!options.useLegacyDatetimeCode && (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3)) {
                    throw new SQLException("Time format \"" + raw + "\" incorrect, must be HH:mm:ss");
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
                    calendar.set(1970, Calendar.JANUARY, 1, (negate ? -1 : 1) * hour, minutes, seconds);
                    int nanoseconds = extractNanos(raw);
                    calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);

                    return new Time(calendar.getTimeInMillis());
                } else {
                    throw new SQLException(raw + " cannot be parse as time. time must have \"99:99:99\" format");
                }
            }
        } else {
            return binaryTime(columnInfo, cal);
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
        checkObjectRange(columnIndex);
        return getInternalTimestamp(columnsInformation[columnIndex - 1], cal);
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
        checkObjectRange(columnIndex);
        return getInternalTimestamp(columnsInformation[columnIndex - 1], null);
    }

    /**
     * Get timeStamp from raw data.
     *
     * @param columnInfo   current column information
     * @param userCalendar user calendar.
     * @return timestamp.
     * @throws SQLException if text value cannot be parse
     */
    @SuppressWarnings("ConstantConditions")
    private Timestamp getInternalTimestamp(ColumnInformation columnInfo, Calendar userCalendar) throws SQLException {
        if (lastValueWasNull()) return null;

        if (!this.isBinaryEncoded) {
            String rawValue = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
            if (rawValue.startsWith("0000-00-00 00:00:00")) {
                lastValueNull |= BIT_LAST_ZERO_DATE;
                return null;
            }

            switch (columnInfo.getColumnType()) {
                case TIME:
                    //time does not go after millisecond
                    Timestamp tt = new Timestamp(getInternalTime(columnInfo, userCalendar).getTime());
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

                        Calendar calendar;
                        if (userCalendar != null) {
                            calendar = userCalendar;
                        } else if (columnInfo.getColumnType().getSqlType() == Types.TIMESTAMP) {
                            calendar = Calendar.getInstance(timeZone);
                        } else {
                            calendar = Calendar.getInstance();
                        }

                        synchronized (calendar) {
                            calendar.clear();
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
                    } catch (NumberFormatException | StringIndexOutOfBoundsException n) {
                        throw new SQLException("Value \"" + rawValue + "\" cannot be parse as Timestamp");
                    }
            }
        } else {
            return binaryTimestamp(columnInfo, userCalendar);
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
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        return new ByteArrayInputStream(new String(row.buf, row.pos, row.getLengthMaxFieldSize(), StandardCharsets.UTF_8).getBytes());
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
        checkObjectRange(columnIndex);
        return getInternalObject(columnsInformation[columnIndex - 1], dataTypeMappingFlags);
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
        throw ExceptionMapper.getFeatureNotSupportedException(
                "Method ResultSet.getObject(int columnIndex, Map<String, Class<?>> map) not supported");
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(
                "Method ResultSet.getObject(String columnLabel, Map<String, Class<?>> map) not supported");
    }


    /**
     * {inheritDoc}.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) throw new SQLException("Class type cannot be null");
        checkObjectRange(columnIndex);
        ColumnInformation col = columnsInformation[columnIndex - 1];

        if (type.equals(String.class)) {
            return (T) getInternalString(col, null);

        } else if (type.equals(Integer.class)) {
            if (lastValueWasNull()) return null;
            return (T) (Integer) getInternalInt(col);

        } else if (type.equals(Long.class)) {
            if (lastValueWasNull()) return null;
            return (T) (Long) getInternalLong(col);

        } else if (type.equals(Short.class)) {
            if (lastValueWasNull()) return null;
            return (T) (Short) getInternalShort(col);

        } else if (type.equals(Double.class)) {
            if (lastValueWasNull()) return null;
            return (T) (Double) getInternalDouble(col);

        } else if (type.equals(Float.class)) {
            if (lastValueWasNull()) return null;
            return (T) (Float) getInternalFloat(col);

        } else if (type.equals(Byte.class)) {
            if (lastValueWasNull()) return null;
            return (T) (Byte) getInternalByte(col);

        } else if (type.equals(byte[].class)) {
            byte[] data = new byte[row.getLengthMaxFieldSize()];
            System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
            return (T) data;

        } else if (type.equals(Date.class)) {
            return (T) getInternalDate(col, null);

        } else if (type.equals(Time.class)) {
            return (T) getInternalTime(col, null);

        } else if (type.equals(Timestamp.class) || type.equals(java.util.Date.class)) {
            return (T) getInternalTimestamp(col, null);

        } else if (type.equals(Boolean.class)) {
            return (T) (Boolean) getInternalBoolean(col);

        } else if (type.equals(Calendar.class)) {
            Calendar calendar = Calendar.getInstance(timeZone);
            Timestamp timestamp = getInternalTimestamp(col, null);
            if (timestamp == null) return null;
            calendar.setTimeInMillis(timestamp.getTime());
            return type.cast(calendar);

        } else if (type.equals(Clob.class) || type.equals(NClob.class)) {
            if (lastValueWasNull()) return null;
            //TODO rewrite Blob to use buffer directly (using offset + length)
            byte[] data = new byte[row.getLengthMaxFieldSize()];
            System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
            return (T) new MariaDbClob(data);

        } else if (type.equals(InputStream.class)) {
            if (lastValueWasNull()) return null;
            return (T) new ByteArrayInputStream(row.buf, row.pos, row.getLengthMaxFieldSize());

        } else if (type.equals(Reader.class)) {
            String value = getInternalString(col);
            if (value == null) return null;
            return (T) new StringReader(value);

        } else if (type.equals(BigDecimal.class)) {
            return (T) getInternalBigDecimal(col);

        } else if (type.equals(BigInteger.class)) {
            return (T) getInternalBigInteger(col);
        } else if (type.equals(BigDecimal.class)) {
            return (T) getInternalBigDecimal(col);

        } else if (type.equals(LocalDateTime.class)) {
            if (lastValueWasNull()) return null;
            ZonedDateTime zonedDateTime = getZonedDateTime(col, LocalDateTime.class);
            return zonedDateTime == null ? null : type.cast(zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime());

        } else if (type.equals(ZonedDateTime.class)) {
            if (lastValueWasNull()) return null;
            return type.cast(getZonedDateTime(col, ZonedDateTime.class));

        } else if (type.equals(OffsetDateTime.class)) {
            if (lastValueWasNull()) return null;
            ZonedDateTime tmpZonedDateTime = getZonedDateTime(col, OffsetDateTime.class);
            return tmpZonedDateTime == null ? null : type.cast(tmpZonedDateTime.toOffsetDateTime());

        } else if (type.equals(OffsetDateTime.class)) {
            if (lastValueWasNull()) return null;
            return type.cast(getLocalDate(col));

        } else if (type.equals(LocalDate.class)) {
            if (lastValueWasNull()) return null;
            return type.cast(getLocalDate(col));

        } else if (type.equals(LocalTime.class)) {
            if (lastValueWasNull()) return null;
            return type.cast(getLocalTime(col));

        } else if (type.equals(OffsetTime.class)) {
            if (lastValueWasNull()) return null;
            return type.cast(getOffsetTime(col));

        }
        throw ExceptionMapper.getFeatureNotSupportedException("Type class '" + type.getName() + "' is not supported");

    }

    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return type.cast(getObject(findColumn(columnLabel), type));
    }

    /**
     * Get object value.
     *
     * @param columnInfo           current column information
     * @param dataTypeMappingFlags dataTypeflag (year is date or int, bit boolean or int,  ...)
     * @return the object value.
     * @throws SQLException if any read error occur
     */
    private Object getInternalObject(ColumnInformation columnInfo, int dataTypeMappingFlags)
            throws SQLException {
        if (lastValueWasNull()) return null;

        switch (columnInfo.getColumnType()) {
            case BIT:
                if (columnInfo.getLength() == 1) {
                    return row.buf[row.pos] != 0;
                }
                byte[] dataBit = new byte[row.length];
                System.arraycopy(row.buf, row.pos, dataBit, 0, row.length);
                return dataBit;
            case TINYINT:
                if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
                    if (!this.isBinaryEncoded) {
                        return row.buf[row.pos] != '0';
                    } else {
                        return row.buf[row.pos] != 0;
                    }
                }
                return getInternalInt(columnInfo);
            case INTEGER:
                if (!columnInfo.isSigned()) {
                    return getInternalLong(columnInfo);
                }
                return getInternalInt(columnInfo);
            case BIGINT:
                if (!columnInfo.isSigned()) {
                    return getInternalBigInteger(columnInfo);
                }
                return getInternalLong(columnInfo);
            case DOUBLE:
                return getInternalDouble(columnInfo);
            case VARCHAR:
                if (columnInfo.isBinary()) {
                    byte[] data = new byte[row.getLengthMaxFieldSize()];
                    System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
                    return data;
                }
                return getInternalString(columnInfo);

            case TIMESTAMP:
            case DATETIME:
                return getInternalTimestamp(columnInfo, null);
            case DATE:
                return getInternalDate(columnInfo, null);
            case DECIMAL:
                return getInternalBigDecimal(columnInfo);
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
                byte[] dataBlob = new byte[row.getLengthMaxFieldSize()];
                System.arraycopy(row.buf, row.pos, dataBlob, 0, row.getLengthMaxFieldSize());
                return dataBlob;
            case NULL:
                return null;
            case YEAR:
                if ((dataTypeMappingFlags & YEAR_IS_DATE_TYPE) != 0) {
                    return getInternalDate(columnInfo, null);
                }
                return getInternalShort(columnInfo);
            case SMALLINT:
            case MEDIUMINT:
                return getInternalInt(columnInfo);
            case FLOAT:
                return getInternalFloat(columnInfo);
            case TIME:
                return getInternalTime(columnInfo, null);
            case VARSTRING:
            case STRING:
                if (columnInfo.isBinary()) {
                    byte[] data = new byte[row.getLengthMaxFieldSize()];
                    System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
                    return data;
                }
                return getInternalString(columnInfo);
            case OLDDECIMAL:
                return getInternalString(columnInfo);
            case GEOMETRY:
                byte[] data = new byte[row.length];
                System.arraycopy(row.buf, row.pos, data, 0, row.length);
                return data;
            case ENUM:
                break;
            case NEWDATE:
                break;
            case SET:
                break;
            default:
                break;
        }
        throw ExceptionMapper.getFeatureNotSupportedException("Type '" + columnInfo.getColumnType().getTypeName() + "' is not supported");
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
        checkObjectRange(columnIndex);
        String value = getInternalString(columnsInformation[columnIndex - 1]);
        if (value == null) return null;
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
    public Ref getRef(int columnIndex) throws SQLException {
        // TODO: figure out what REF's are and implement this method
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
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
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        //TODO implement MariaDbBlob with offset
        byte[] data = new byte[row.getLengthMaxFieldSize()];
        System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
        return new MariaDbBlob(data);
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
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        //TODO implement MariaDbClob with offset
        byte[] data = new byte[row.getLengthMaxFieldSize()];
        System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
        return new MariaDbClob(data);
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
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        try {
            return new URL(getInternalString(columnsInformation[columnIndex - 1]));
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
    public NClob getNClob(int columnIndex) throws SQLException {
        checkObjectRange(columnIndex);
        if (lastValueWasNull()) return null;
        //TODO implement MariaDbBlob with offset
        byte[] data = new byte[row.getLengthMaxFieldSize()];
        System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
        return new MariaDbClob(data);
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
    public boolean getBoolean(int index) throws SQLException {
        checkObjectRange(index);
        return getInternalBoolean(columnsInformation[index - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }


    /**
     * {inheritDoc}.
     */
    public byte getByte(int index) throws SQLException {
        checkObjectRange(index);
        return getInternalByte(columnsInformation[index - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public short getShort(int index) throws SQLException {
        checkObjectRange(index);
        return getInternalShort(columnsInformation[index - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
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
    public void insertRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("insertRow are not supported when using ResultSet.CONCUR_READ_ONLY");
    }

    /**
     * {inheritDoc}.
     */
    public void deleteRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("deleteRow are not supported when using ResultSet.CONCUR_READ_ONLY");
    }

    /**
     * {inheritDoc}.
     */
    public void refreshRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("refreshRow are not supported when using ResultSet.CONCUR_READ_ONLY");
    }

    /**
     * {inheritDoc}.
     */
    public void cancelRowUpdates() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void moveToInsertRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void moveToCurrentRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNull(int columnIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNull(String columnLabel) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBoolean(int columnIndex, boolean bool) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBoolean(String columnLabel, boolean value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateByte(int columnIndex, byte value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateByte(String columnLabel, byte value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateShort(int columnIndex, short value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateShort(String columnLabel, short value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateInt(int columnIndex, int value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateInt(String columnLabel, int value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateFloat(int columnIndex, float value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateFloat(String columnLabel, float value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateDouble(int columnIndex, double value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateDouble(String columnLabel, double value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBigDecimal(int columnIndex, BigDecimal value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBigDecimal(String columnLabel, BigDecimal value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateString(int columnIndex, String value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateString(String columnLabel, String value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBytes(int columnIndex, byte[] value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBytes(String columnLabel, byte[] value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateDate(int columnIndex, Date date) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateDate(String columnLabel, Date value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateTime(int columnIndex, Time time) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateTime(String columnLabel, Time value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateTimestamp(int columnIndex, Timestamp timeStamp) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateTimestamp(String columnLabel, Timestamp value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(String columnLabel, InputStream value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(String columnLabel, InputStream value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBinaryStream(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(int columnIndex, Reader value, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(int columnIndex, Reader value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(int columnIndex, Reader value, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(int columnIndex, Object value, int scaleOrLength) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(int columnIndex, Object value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(String columnLabel, Object value, int scaleOrLength) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateObject(String columnLabel, Object value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateLong(String columnLabel, long value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateLong(int columnIndex, long value) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateRow() throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("updateRow are not supported when using ResultSet.CONCUR_READ_ONLY");
    }

    /**
     * {inheritDoc}.
     */
    public void updateRef(int columnIndex, Ref ref) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateRef(String columnLabel, Ref ref) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(int columnIndex, Blob blob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(String columnLabel, Blob blob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(int columnIndex, Clob clob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(String columnLabel, Clob clob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateArray(int columnIndex, Array array) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateArray(String columnLabel, Array array) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateRowId(int columnIndex, RowId rowId) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);

    }

    /**
     * {inheritDoc}.
     */
    public void updateRowId(String columnLabel, RowId rowId) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);

    }

    /**
     * {inheritDoc}.
     */
    public void updateNString(int columnIndex, String nstring) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNString(String columnLabel, String nstring) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(int columnIndex, NClob nclob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(String columnLabel, NClob nclob) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
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
    public void updateNCharacterStream(int columnIndex, Reader value, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
    }

    /**
     * {inheritDoc}.
     */
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * Get boolean value from raw data.
     *
     * @param columnInfo current column information
     * @return boolean
     * @throws SQLException id any error occur
     */
    private boolean getInternalBoolean(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return false;

        if (!this.isBinaryEncoded) {
            if (row.length == 1 && row.buf[row.pos] == 0) {
                return false;
            }
            final String rawVal = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
            return !("false".equals(rawVal) || "0".equals(rawVal));
        } else {
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos] != 0;
                case TINYINT:
                    return getInternalTinyInt(columnInfo) != 0;
                case SMALLINT:
                case YEAR:
                    return getInternalSmallInt(columnInfo) != 0;
                case INTEGER:
                case MEDIUMINT:
                    return getInternalMediumInt(columnInfo) != 0;
                case BIGINT:
                    return getInternalLong(columnInfo) != 0;
                case FLOAT:
                    return getInternalFloat(columnInfo) != 0;
                case DOUBLE:
                    return getInternalDouble(columnInfo) != 0;
                default:
                    final String rawVal = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                    return !("false".equals(rawVal) || "0".equals(rawVal));
            }
        }
    }

    /**
     * Get byte from raw data.
     *
     * @param columnInfo current column information
     * @return byte
     * @throws SQLException id any error occur
     */
    private byte getInternalByte(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;

        if (!this.isBinaryEncoded) {
            if (columnInfo.getColumnType() == ColumnType.BIT) {
                return row.buf[row.pos];
            }
            return parseByte(columnInfo);
        } else {
            long value;
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                    value = getInternalTinyInt(columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = getInternalSmallInt(columnInfo);
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getInternalMediumInt(columnInfo);
                    break;
                case BIGINT:
                    value = getInternalLong(columnInfo);
                    break;
                case FLOAT:
                    value = (long) getInternalFloat(columnInfo);
                    break;
                case DOUBLE:
                    value = (long) getInternalDouble(columnInfo);
                    break;
                default:
                    return parseByte(columnInfo);
            }
            rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, value, columnInfo);
            return (byte) value;
        }
    }

    /**
     * Get short from raw data.
     *
     * @param columnInfo current column information
     * @return short
     * @throws SQLException id any error occur
     */
    private short getInternalShort(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;

        if (!this.isBinaryEncoded) {
            return parseShort(columnInfo);
        } else {
            long value;
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return row.buf[row.pos];
                case TINYINT:
                    value = getInternalTinyInt(columnInfo);
                    break;
                case SMALLINT:
                case YEAR:
                    value = ((row.buf[row.pos] & 0xff) + ((row.buf[row.pos + 1] & 0xff) << 8));
                    if (columnInfo.isSigned()) {
                        return (short) value;
                    }
                    value = value & 0xffff;
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getInternalMediumInt(columnInfo);
                    break;
                case BIGINT:
                    value = getInternalLong(columnInfo);
                    break;
                case FLOAT:
                    value = (long) getInternalFloat(columnInfo);
                    break;
                case DOUBLE:
                    value = (long) getInternalDouble(columnInfo);
                    break;
                default:
                    return parseShort(columnInfo);
            }
            rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, value, columnInfo);
            return (short) value;
        }
    }

    /**
     * {inheritDoc}.
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException("The receiver is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException("The receiver is not a wrapper and does not implement the interface");
        }
    }

    /**
     * {inheritDoc}.
     */
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    public void setReturnTableAlias(boolean returnTableAlias) {
        this.returnTableAlias = returnTableAlias;
    }

    private String getTimeString(ColumnInformation columnInfo) {
        if (lastValueWasNull()) return null;
        if (row.length == 0) {
            // binary send 00:00:00 as 0.
            if (columnInfo.getDecimals() == 0) {
                return "00:00:00";
            } else {
                StringBuilder value = new StringBuilder("00:00:00.");
                int decimal = columnInfo.getDecimals();
                while (decimal-- > 0) value.append("0");
                return value.toString();
            }
        }
        String rawValue = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
        if ("0000-00-00".equals(rawValue)) return null;

        if (!this.isBinaryEncoded) {
            if (options.maximizeMysqlCompatibility && options.useLegacyDatetimeCode && rawValue.indexOf(".") > 0) {
                return rawValue.substring(0, rawValue.indexOf("."));
            }
            return rawValue;
        }
        int day = ((row.buf[row.pos + 1] & 0xff)
                | ((row.buf[row.pos + 2] & 0xff) << 8)
                | ((row.buf[row.pos + 3] & 0xff) << 16)
                | ((row.buf[row.pos + 4] & 0xff) << 24));
        int hour = row.buf[row.pos + 5];
        int timeHour = hour + day * 24;

        String hourString;
        if (timeHour < 10) {
            hourString = "0" + timeHour;
        } else {
            hourString = Integer.toString(timeHour);
        }

        String minuteString;
        int minutes = row.buf[row.pos + 6];
        if (minutes < 10) {
            minuteString = "0" + minutes;
        } else {
            minuteString = Integer.toString(minutes);
        }

        String secondString;
        int seconds = row.buf[row.pos + 7];
        if (seconds < 10) {
            secondString = "0" + seconds;
        } else {
            secondString = Integer.toString(seconds);
        }

        int microseconds = 0;
        if (row.length > 8) {
            microseconds = ((row.buf[row.pos + 8] & 0xff)
                    | (row.buf[row.pos + 9] & 0xff) << 8
                    | (row.buf[row.pos + 10] & 0xff) << 16
                    | (row.buf[row.pos + 11] & 0xff) << 24);
        }

        StringBuilder microsecondString = new StringBuilder(Integer.toString(microseconds));
        while (microsecondString.length() < 6) {
            microsecondString.insert(0, "0");
        }
        boolean negative = (row.buf[row.pos] == 0x01);
        return (negative ? "-" : "") + (hourString + ":" + minuteString + ":" + secondString + "." + microsecondString);
    }


    private void rangeCheck(Object className, long minValue, long maxValue, long value, ColumnInformation columnInfo) throws SQLException {
        if (value < minValue || value > maxValue) {
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value + " is not in "
                    + className + " range", "22003", 1264);
        }
    }

    private int getInternalTinyInt(ColumnInformation columnInfo) {
        if (lastValueWasNull()) return 0;
        int value = row.buf[row.pos];
        if (!columnInfo.isSigned()) {
            value = (row.buf[row.pos] & 0xff);
        }
        return value;
    }

    private int getInternalSmallInt(ColumnInformation columnInfo) {
        if (lastValueWasNull()) return 0;
        int value = ((row.buf[row.pos] & 0xff) + ((row.buf[row.pos + 1] & 0xff) << 8));
        if (!columnInfo.isSigned()) {
            return value & 0xffff;
        }
        //short cast here is important : -1 will be received as -1, -1 -> 65535
        return (short) value;
    }

    private long getInternalMediumInt(ColumnInformation columnInfo) {
        if (lastValueWasNull()) return 0;
        long value = ((row.buf[row.pos] & 0xff)
                + ((row.buf[row.pos + 1] & 0xff) << 8)
                + ((row.buf[row.pos + 2] & 0xff) << 16)
                + ((row.buf[row.pos + 3] & 0xff) << 24));
        if (!columnInfo.isSigned()) {
            value = value & 0xffffffffL;
        }
        return value;
    }


    private byte parseByte(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        try {
            switch (columnInfo.getColumnType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Byte.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + " is not in Byte range", "22003", 1264);
                    }
                    return floatValue.byteValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Byte.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + " is not in Byte range", "22003", 1264);
                    }
                    return doubleValue.byteValue();
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                    long result = 0;
                    int length = row.length;
                    boolean negate = false;
                    int begin = row.pos;
                    if (length > 0 && row.buf[begin] == 45) { //minus sign
                        negate = true;
                        begin++;
                    }
                    for (; begin < row.pos + length; begin++) {
                        result = result * 10 + row.buf[begin] - 48;
                    }
                    result = (negate ? -1 * result : result);
                    rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, result, columnInfo);
                    return (byte) result;
                default:
                    return Byte.parseByte(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if its a decimal retry without the decimal part.
            String value = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
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

    private short parseShort(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        try {
            switch (columnInfo.getColumnType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Short.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + " is not in Short range", "22003", 1264);
                    }
                    return floatValue.shortValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Short.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
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
                    int length = row.length;
                    boolean negate = false;
                    int begin = row.pos;
                    if (length > 0 && row.buf[begin] == 45) { //minus sign
                        negate = true;
                        begin++;
                    }
                    for (; begin < row.pos + length; begin++) {
                        result = result * 10 + row.buf[begin] - 48;
                    }
                    result = (negate ? -1 * result : result);
                    rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, result, columnInfo);
                    return (short) result;
                default:
                    return Short.parseShort(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if its a decimal retry without the decimal part.
            String value = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
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


    private int parseInt(ColumnInformation columnInfo) throws SQLException {
        try {
            switch (columnInfo.getColumnType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Integer.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + " is not in Integer range", "22003", 1264);
                    }
                    return floatValue.intValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Integer.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
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
                    boolean negate = false;
                    int begin = row.pos;
                    if (row.length > 0 && row.buf[begin] == 45) { //minus sign
                        negate = true;
                        begin++;
                    }
                    for (; begin < row.pos + row.length; begin++) {
                        result = result * 10 + row.buf[begin] - 48;
                    }
                    //specific for BIGINT : if value > Long.MAX_VALUE will become negative.
                    if (result < 0) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + " is not in Integer range", "22003", 1264);
                    }
                    result = (negate ? -1 * result : result);
                    rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, result, columnInfo);
                    return (int) result;
                default:
                    return Integer.parseInt(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if its a decimal retry without the decimal part.
            String value = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
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

    private long parseLong(ColumnInformation columnInfo) throws SQLException {
        try {
            switch (columnInfo.getColumnType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
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
                    int length = row.length;
                    boolean negate = false;
                    int begin = row.pos;
                    if (length > 0 && row.buf[begin] == 45) { //minus sign
                        negate = true;
                        begin++;
                    }
                    for (; begin < row.pos + length; begin++) {
                        result = result * 10 + row.buf[begin] - 48;
                    }
                    //specific for BIGINT : if value > Long.MAX_VALUE , will become negative until -1
                    if (result < 0) {
                        //CONJ-399 : handle specifically Long.MIN_VALUE that has absolute value +1 compare to LONG.MAX_VALUE
                        if (result == Long.MIN_VALUE && negate) return Long.MIN_VALUE;
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8)
                                + " is not in Long range", "22003", 1264);
                    }
                    return (negate ? -1 * result : result);
                default:
                    return Long.parseLong(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
            }

        } catch (NumberFormatException nfe) {
            //parse error.
            //if its a decimal retry without the decimal part.
            String value = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
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
     * @param columnInfo current column information
     * @return bigInteger
     * @throws SQLException exception
     */
    private BigInteger getInternalBigInteger(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return null;
        if (!this.isBinaryEncoded) {
            return new BigInteger(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
        } else {
            switch (columnInfo.getColumnType()) {
                case BIT:
                    return BigInteger.valueOf((long) row.buf[row.pos]);
                case TINYINT:
                    return BigInteger.valueOf((long)
                            (columnInfo.isSigned() ? row.buf[row.pos] : (row.buf[row.pos] & 0xff)));
                case SMALLINT:
                case YEAR:
                    short valueShort = (short) ((row.buf[row.pos] & 0xff) | ((row.buf[row.pos + 1] & 0xff) << 8));
                    return BigInteger.valueOf((long) (columnInfo.isSigned() ? valueShort : (valueShort & 0xffff)));
                case INTEGER:
                case MEDIUMINT:
                    int valueInt = ((row.buf[row.pos] & 0xff)
                            + ((row.buf[row.pos + 1] & 0xff) << 8)
                            + ((row.buf[row.pos + 2] & 0xff) << 16)
                            + ((row.buf[row.pos + 3] & 0xff) << 24));
                    return BigInteger.valueOf(((columnInfo.isSigned()) ? valueInt : (valueInt >= 0) ? valueInt : valueInt & 0xffffffffL));
                case BIGINT:
                    long value = ((row.buf[row.pos] & 0xff)
                            + ((long) (row.buf[row.pos + 1] & 0xff) << 8)
                            + ((long) (row.buf[row.pos + 2] & 0xff) << 16)
                            + ((long) (row.buf[row.pos + 3] & 0xff) << 24)
                            + ((long) (row.buf[row.pos + 4] & 0xff) << 32)
                            + ((long) (row.buf[row.pos + 5] & 0xff) << 40)
                            + ((long) (row.buf[row.pos + 6] & 0xff) << 48)
                            + ((long) (row.buf[row.pos + 7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return BigInteger.valueOf(value);
                    } else {
                        return new BigInteger(1, new byte[]{(byte) (value >> 56),
                                (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                                (byte) value});
                    }
                case FLOAT:
                    return BigInteger.valueOf((long) getInternalFloat(columnInfo));
                case DOUBLE:
                    return BigInteger.valueOf((long) getInternalDouble(columnInfo));
                default:
                    return new BigInteger(new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8));
            }
        }

    }

    private Date binaryDate(ColumnInformation columnInfo, Calendar cal) throws SQLException {
        switch (columnInfo.getColumnType()) {
            case TIMESTAMP:
            case DATETIME:
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal);
                return (timestamp == null) ? null : new Date(timestamp.getTime());
            case TIME:
                throw new SQLException("Cannot read Date using a Types.TIME field");
            default:
                if (row.length == 0) {
                    lastValueNull |= BIT_LAST_FIELD_NULL;
                    return null;
                }

                int year = ((row.buf[row.pos] & 0xff) | (row.buf[row.pos + 1] & 0xff) << 8);

                if (row.length == 2 && columnInfo.getLength() == 2) {
                    //YEAR(2) - deprecated
                    if (year <= 69) {
                        year += 2000;
                    } else {
                        year += 1900;
                    }
                }

                int month = 1;
                int day = 1;

                if (row.length >= 4) {
                    month = row.buf[row.pos + 2];
                    day = row.buf[row.pos + 3];
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

    private Time binaryTime(ColumnInformation columnInfo, Calendar cal) throws SQLException {
        switch (columnInfo.getColumnType()) {
            case TIMESTAMP:
            case DATETIME:
                Timestamp ts = binaryTimestamp(columnInfo, cal);
                return (ts == null) ? null : new Time(ts.getTime());
            case DATE:
                throw new SQLException("Cannot read Time using a Types.DATE field");
            default:
                Calendar calendar = Calendar.getInstance();
                calendar.clear();
                int day = 0;
                int hour = 0;
                int minutes = 0;
                int seconds = 0;
                boolean negate = false;
                if (row.length > 0) {
                    negate = (row.buf[row.pos] & 0xff) == 0x01;
                }
                if (row.length > 4) {
                    day = ((row.buf[row.pos + 1] & 0xff)
                            + ((row.buf[row.pos + 2] & 0xff) << 8)
                            + ((row.buf[row.pos + 3] & 0xff) << 16)
                            + ((row.buf[row.pos + 4] & 0xff) << 24));
                }
                if (row.length > 7) {
                    hour = row.buf[row.pos + 5];
                    minutes = row.buf[row.pos + 6];
                    seconds = row.buf[row.pos + 7];
                }
                calendar.set(1970, Calendar.JANUARY, ((negate ? -1 : 1) * day) + 1, (negate ? -1 : 1) * hour, minutes, seconds);

                int nanoseconds = 0;
                if (row.length > 8) {
                    nanoseconds = ((row.buf[row.pos + 8] & 0xff)
                            + ((row.buf[row.pos + 9] & 0xff) << 8)
                            + ((row.buf[row.pos + 10] & 0xff) << 16)
                            + ((row.buf[row.pos + 11] & 0xff) << 24));
                }

                calendar.set(Calendar.MILLISECOND, nanoseconds / 1000);

                return new Time(calendar.getTimeInMillis());
        }
    }


    private Timestamp binaryTimestamp(ColumnInformation columnInfo, Calendar userCalendar) {
        if (row.length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        int year;
        int month;
        int day = 0;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        if (columnInfo.getColumnType() == ColumnType.TIME) {
            Calendar calendar = userCalendar != null ? userCalendar : Calendar.getInstance();

            boolean negate = false;
            if (row.length > 0) {
                negate = (row.buf[row.pos] & 0xff) == 0x01;
            }
            if (row.length > 4) {
                day = ((row.buf[row.pos + 1] & 0xff)
                        + ((row.buf[row.pos + 2] & 0xff) << 8)
                        + ((row.buf[row.pos + 3] & 0xff) << 16)
                        + ((row.buf[row.pos + 4] & 0xff) << 24));
            }
            if (row.length > 7) {
                hour = row.buf[row.pos + 5];
                minutes = row.buf[row.pos + 6];
                seconds = row.buf[row.pos + 7];
            }

            if (row.length > 8) {
                microseconds = ((row.buf[row.pos + 8] & 0xff)
                        + ((row.buf[row.pos + 9] & 0xff) << 8)
                        + ((row.buf[row.pos + 10] & 0xff) << 16)
                        + ((row.buf[row.pos + 11] & 0xff) << 24));
            }

            Timestamp tt;
            synchronized (calendar) {
                calendar.clear();
                calendar.set(1970, Calendar.JANUARY, ((negate ? -1 : 1) * day) + 1, (negate ? -1 : 1) * hour, minutes, seconds);
                tt = new Timestamp(calendar.getTimeInMillis());
            }
            tt.setNanos(microseconds * 1000);
            return tt;
        } else {
            year = ((row.buf[row.pos] & 0xff) | (row.buf[row.pos + 1] & 0xff) << 8);
            month = row.buf[row.pos + 2];
            day = row.buf[row.pos + 3];
            if (row.length > 4) {
                hour = row.buf[row.pos + 4];
                minutes = row.buf[row.pos + 5];
                seconds = row.buf[row.pos + 6];

                if (row.length > 7) {
                    microseconds = ((row.buf[row.pos + 7] & 0xff)
                            + ((row.buf[row.pos + 8] & 0xff) << 8)
                            + ((row.buf[row.pos + 9] & 0xff) << 16)
                            + ((row.buf[row.pos + 10] & 0xff) << 24));
                }
            }
        }

        Calendar calendar;
        if (userCalendar != null) {
            calendar = userCalendar;
        } else if (columnInfo.getColumnType().getSqlType() == Types.TIMESTAMP) {
            calendar = Calendar.getInstance(timeZone);
        } else {
            calendar = Calendar.getInstance();
        }

        Timestamp tt;
        synchronized (calendar) {
            calendar.clear();
            calendar.set(year, month - 1, day, hour, minutes, seconds);
            tt = new Timestamp(calendar.getTimeInMillis());
        }

        tt.setNanos(microseconds * 1000);
        return tt;
    }

    private int extractNanos(String timestring) throws SQLException {
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
                    throw new SQLException("cannot parse sub-second part in timestamp string '" + timestring + "'");
                }
                digit = value - '0';
            }
            nanos = nanos * 10 + digit;
        }
        return nanos;
    }


    /**
     * Get LocalDateTime from raw data.
     *
     * @param columnInfo current column information
     * @param clazz      ending class
     * @return timestamp.
     * @throws SQLException if any read error occur
     */
    private ZonedDateTime getZonedDateTime(ColumnInformation columnInfo, Class clazz) throws SQLException {
        if (lastValueWasNull()) return null;
        if (row.length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        if (!this.isBinaryEncoded) {

            String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIMESTAMP:

                    if (raw.startsWith("0000-00-00 00:00:00")) return null;
                    try {
                        LocalDateTime localDateTime = LocalDateTime.parse(raw, TEXT_LOCAL_DATE_TIME.withZone(timeZone.toZoneId()));
                        return ZonedDateTime.of(localDateTime, timeZone.toZoneId());
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalDateTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                    }

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:

                    if (raw.startsWith("0000-00-00 00:00:00")) return null;
                    try {
                        return ZonedDateTime.parse(raw, TEXT_ZONED_DATE_TIME);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as ZonedDateTime. time must have \"yyyy-MM-dd[T/ ]HH:mm:ss[.S]\" "
                                + "with offset and timezone format (example : '2011-12-03 10:15:30+01:00[Europe/Paris]')");
                    }

                default:
                    throw new SQLException("Cannot read " + clazz.getName() + " using a " + columnInfo.getColumnType().getJavaTypeName() + " field");

            }

        } else {

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIMESTAMP:

                    int year = ((row.buf[row.pos] & 0xff) | (row.buf[row.pos + 1] & 0xff) << 8);
                    int month = row.buf[row.pos + 2];
                    int day = row.buf[row.pos + 3];
                    int hour = 0;
                    int minutes = 0;
                    int seconds = 0;
                    int microseconds = 0;

                    if (row.length > 4) {
                        hour = row.buf[row.pos + 4];
                        minutes = row.buf[row.pos + 5];
                        seconds = row.buf[row.pos + 6];

                        if (row.length > 7) {
                            microseconds = ((row.buf[row.pos + 7] & 0xff)
                                    + ((row.buf[row.pos + 8] & 0xff) << 8)
                                    + ((row.buf[row.pos + 9] & 0xff) << 16)
                                    + ((row.buf[row.pos + 10] & 0xff) << 24));
                        }
                    }

                    return ZonedDateTime.of(year, month, day, hour, minutes, seconds, microseconds * 1000, timeZone.toZoneId());

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:

                    //string conversion
                    String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                    if (raw.startsWith("0000-00-00 00:00:00")) return null;
                    try {
                        return ZonedDateTime.parse(raw, TEXT_ZONED_DATE_TIME);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as ZonedDateTime. time must have \"yyyy-MM-dd[T/ ]HH:mm:ss[.S]\" "
                                + "with offset and timezone format (example : '2011-12-03 10:15:30+01:00[Europe/Paris]')");
                    }

                default:
                    throw new SQLException("Cannot read " + clazz.getName() + " using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        }
    }


    /**
     * Get OffsetTime from raw data.
     *
     * @param columnInfo current column information
     * @return timestamp.
     * @throws SQLException if any read error occur
     */
    private OffsetTime getOffsetTime(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return null;
        if (row.length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        ZoneId zoneId = timeZone.toZoneId().normalized();
        if (ZoneOffset.class.isInstance(zoneId)) {
            ZoneOffset zoneOffset = ZoneOffset.class.cast(zoneId);
            if (!this.isBinaryEncoded) {
                String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                switch (columnInfo.getColumnType().getSqlType()) {

                    case Types.TIMESTAMP:
                        if (raw.startsWith("0000-00-00 00:00:00")) return null;
                        try {
                            return ZonedDateTime.parse(raw, TEXT_LOCAL_DATE_TIME.withZone(zoneOffset)).toOffsetDateTime().toOffsetTime();
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                        }

                    case Types.TIME:
                        try {
                            LocalTime localTime = LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(zoneOffset));
                            return OffsetTime.of(localTime, zoneOffset);
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" for data type \""
                                    + columnInfo.getColumnType() + "\")");
                        }

                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CHAR:
                        try {
                            return OffsetTime.parse(raw, DateTimeFormatter.ISO_OFFSET_TIME);
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" with offset for data type \""
                                    + columnInfo.getColumnType() + "\")");
                        }

                    default:
                        throw new SQLException("Cannot read " + OffsetTime.class.getName() + " using a "
                                + columnInfo.getColumnType().getJavaTypeName() + " field");
                }

            } else {

                int day = 0;
                int hour = 0;
                int minutes = 0;
                int seconds = 0;
                int microseconds = 0;

                switch (columnInfo.getColumnType().getSqlType()) {
                    case Types.TIMESTAMP:
                        int year = ((row.buf[row.pos] & 0xff) | (row.buf[row.pos + 1] & 0xff) << 8);
                        int month = row.buf[row.pos + 2];
                        day = row.buf[row.pos + 3];

                        if (row.length > 4) {
                            hour = row.buf[row.pos + 4];
                            minutes = row.buf[row.pos + 5];
                            seconds = row.buf[row.pos + 6];

                            if (row.length > 7) {
                                microseconds = ((row.buf[row.pos + 7] & 0xff)
                                        + ((row.buf[row.pos + 8] & 0xff) << 8)
                                        + ((row.buf[row.pos + 9] & 0xff) << 16)
                                        + ((row.buf[row.pos + 10] & 0xff) << 24));
                            }
                        }

                        return ZonedDateTime.of(year, month, day, hour, minutes, seconds, microseconds * 1000, zoneOffset)
                                .toOffsetDateTime().toOffsetTime();

                    case Types.TIME:

                        boolean negate = (row.buf[row.pos] & 0xff) == 0x01;

                        if (row.length > 4) {
                            day = ((row.buf[row.pos + 1] & 0xff)
                                    + ((row.buf[row.pos + 2] & 0xff) << 8)
                                    + ((row.buf[row.pos + 3] & 0xff) << 16)
                                    + ((row.buf[row.pos + 4] & 0xff) << 24));
                        }

                        if (row.length > 7) {
                            hour = row.buf[row.pos + 5];
                            minutes = row.buf[row.pos + 6];
                            seconds = row.buf[row.pos + 7];
                        }

                        if (row.length > 8) {
                            microseconds = ((row.buf[row.pos + 8] & 0xff)
                                    + ((row.buf[row.pos + 9] & 0xff) << 8)
                                    + ((row.buf[row.pos + 10] & 0xff) << 16)
                                    + ((row.buf[row.pos + 11] & 0xff) << 24));
                        }

                        return OffsetTime.of((negate ? -1 : 1) * (day * 24 + hour), minutes, seconds, microseconds * 1000, zoneOffset);

                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CHAR:
                        String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                        try {
                            return OffsetTime.parse(raw, DateTimeFormatter.ISO_OFFSET_TIME);
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" with offset for data type \""
                                    + columnInfo.getColumnType() + "\")");
                        }

                    default:
                        throw new SQLException("Cannot read " + OffsetTime.class.getName() + " using a "
                                + columnInfo.getColumnType().getJavaTypeName() + " field");
                }
            }
        }

        if (options.useLegacyDatetimeCode) {
            //system timezone is not an offset
            throw new SQLException("Cannot return an OffsetTime for a TIME field when default timezone is '" + zoneId
                    + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
        }

        //server timezone is not an offset
        throw new SQLException("Cannot return an OffsetTime for a TIME field when server timezone '" + zoneId
                + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");

    }

    /**
     * Get LocalTime from raw data.
     *
     * @param columnInfo current column information
     * @return timestamp.
     * @throws SQLException if any read error occur
     */
    private LocalTime getLocalTime(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return null;
        if (row.length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        if (!this.isBinaryEncoded) {

            String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIME:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    try {
                        return LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(columnInfo, LocalTime.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalTime();

                default:
                    throw new SQLException("Cannot read LocalTime using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        } else {


            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIME:

                    int day = 0;
                    int hour = 0;
                    int minutes = 0;
                    int seconds = 0;
                    int microseconds = 0;

                    boolean negate = (row.buf[row.pos] & 0xff) == 0x01;

                    if (row.length > 4) {
                        day = ((row.buf[row.pos + 1] & 0xff)
                                + ((row.buf[row.pos + 2] & 0xff) << 8)
                                + ((row.buf[row.pos + 3] & 0xff) << 16)
                                + ((row.buf[row.pos + 4] & 0xff) << 24));
                    }

                    if (row.length > 7) {
                        hour = row.buf[row.pos + 5];
                        minutes = row.buf[row.pos + 6];
                        seconds = row.buf[row.pos + 7];
                    }

                    if (row.length > 8) {
                        microseconds = ((row.buf[row.pos + 8] & 0xff)
                                + ((row.buf[row.pos + 9] & 0xff) << 8)
                                + ((row.buf[row.pos + 10] & 0xff) << 16)
                                + ((row.buf[row.pos + 11] & 0xff) << 24));
                    }

                    return LocalTime.of((negate ? -1 : 1) * (day * 24 + hour), minutes, seconds, microseconds * 1000);

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    //string conversion
                    String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                    try {
                        return LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(columnInfo, LocalTime.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalTime();

                default:
                    throw new SQLException("Cannot read LocalTime using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        }
    }


    /**
     * Get LocalDateTime from raw data.
     *
     * @param columnInfo current column information
     * @return timestamp.
     * @throws SQLException if any read error occur
     */
    private LocalDate getLocalDate(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return null;
        if (row.length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        if (!this.isBinaryEncoded) {

            String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.DATE:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    if (raw.startsWith("0000-00-00")) return null;
                    try {
                        return LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalDate (format is \"yyyy-MM-dd\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(columnInfo, LocalDate.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDate();

                default:
                    throw new SQLException("Cannot read LocalDate using a " + columnInfo.getColumnType().getJavaTypeName() + " field");

            }

        } else {

            switch (columnInfo.getColumnType().getSqlType()) {

                case Types.DATE:
                    int year = ((row.buf[row.pos] & 0xff) | (row.buf[row.pos + 1] & 0xff) << 8);
                    int month = row.buf[row.pos + 2];
                    int day = row.buf[row.pos + 3];
                    return LocalDate.of(year, month, day);

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(columnInfo, LocalDate.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDate();

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    //string conversion
                    String raw = new String(row.buf, row.pos, row.length, StandardCharsets.UTF_8);
                    if (raw.startsWith("0000-00-00")) return null;
                    try {
                        return LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalDate. time must have \"yyyy-MM-dd\" format");
                    }

                default:
                    throw new SQLException("Cannot read LocalDate using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        }
    }

    public int getRowPointer() {
        return rowPointer;
    }

    protected void setRowPointer(int pointer) {
        rowPointer = pointer;
    }

    public int getDataSize() {
        return dataSize;
    }
}
