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

import static org.mariadb.jdbc.internal.com.Packet.EOF;
import static org.mariadb.jdbc.internal.com.Packet.ERROR;
import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;
import static org.mariadb.jdbc.internal.util.constant.ServerStatus.MORE_RESULTS_EXISTS;
import static org.mariadb.jdbc.internal.util.constant.ServerStatus.PS_OUT_PARAMETERS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;
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

@SuppressWarnings({"deprecation", "BigDecimalMethodWithoutRoundingCalled",
    "StatementWithEmptyBody", "SynchronizationOnLocalVariableOrMethodParameter"})
public class SelectResultSet implements ResultSet {

  public static final int TINYINT1_IS_BIT = 1;
  public static final int YEAR_IS_DATE_TYPE = 2;
  private static final String NOT_UPDATABLE_ERROR = "Updates are not supported when using ResultSet.CONCUR_READ_ONLY";
  private static final ColumnInformation[] INSERT_ID_COLUMNS;

  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  static {
    INSERT_ID_COLUMNS = new ColumnInformation[1];
    INSERT_ID_COLUMNS[0] = ColumnInformation.create("insert_id", ColumnType.BIGINT);
  }

  protected TimeZone timeZone;
  protected Options options;
  protected ColumnInformation[] columnsInformation;
  protected int columnInformationLength;
  protected boolean noBackslashEscapes;
  private Protocol protocol;
  private PacketInputStream reader;
  private boolean isEof;
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
  private int lastRowPointer = -1;
  private int dataTypeMappingFlags;
  private boolean returnTableAlias;
  private boolean isClosed;
  private boolean eofDeprecated;
  private ReentrantLock lock;

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
    this.returnTableAlias = this.options.useOldAliasMetadataBehavior;
    this.columnsInformation = columnInformation;
    this.columnNameMap = new ColumnNameMap(columnsInformation);

    this.columnInformationLength = columnInformation.length;
    this.reader = reader;
    this.isEof = false;
    timeZone = protocol.getTimeZone();
    if (results.isBinaryFormat()) {
      row = new BinaryRowProtocol(columnsInformation, columnInformationLength,
          results.getMaxFieldSize(), options);
    } else {
      row = new TextRowProtocol(results.getMaxFieldSize(), options);
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
      this.lock = protocol.getLock();
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
   * @param resultSetScrollType one of the following <code>ResultSet</code> constants:
   *                            <code>ResultSet.TYPE_FORWARD_ONLY</code>,
   *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
   *                            <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   */
  public SelectResultSet(ColumnInformation[] columnInformation, List<byte[]> resultSet,
      Protocol protocol,
      int resultSetScrollType) {
    this.statement = null;
    this.isClosed = false;
    if (protocol != null) {
      this.options = protocol.getOptions();
      this.timeZone = protocol.getTimeZone();
      this.returnTableAlias = this.options.useOldAliasMetadataBehavior;
    } else {
      this.options = new Options();
      this.timeZone = TimeZone.getDefault();
      this.returnTableAlias = false;
    }
    this.row = new TextRowProtocol(0, this.options);
    this.protocol = null;
    this.columnsInformation = columnInformation;
    this.columnNameMap = new ColumnNameMap(columnsInformation);
    this.columnInformationLength = columnInformation.length;
    this.isEof = true;
    this.fetchSize = 0;
    this.resultSetScrollType = resultSetScrollType;
    this.data = resultSet.toArray(new byte[10][]);
    this.dataSize = resultSet.size();
    this.dataFetchTime = 0;
    this.rowPointer = -1;
    this.callableResult = false;
    this.streaming = false;
  }

  /**
   * Create a result set from given data. Useful for creating "fake" resultsets for
   * DatabaseMetaData, (one example is MariaDbDatabaseMetaData.getTypeInfo())
   *
   * @param data                 - each element of this array represents a complete row in the
   *                             ResultSet. Each value is given in its string representation, as in
   *                             MariaDB text protocol, except boolean (BIT(1)) values that are
   *                             represented as "1" or "0" strings
   * @param protocol             protocol
   * @param findColumnReturnsOne - special parameter, used only in generated key result sets
   * @return resultset
   */
  public static ResultSet createGeneratedData(long[] data, Protocol protocol,
      boolean findColumnReturnsOne) {
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
   * Create a result set from given data. Useful for creating "fake" resultSets for
   * DatabaseMetaData, (one example is MariaDbDatabaseMetaData.getTypeInfo())
   *
   * @param columnNames - string array of column names
   * @param columnTypes - column types
   * @param data        - each element of this array represents a complete row in the ResultSet.
   *                    Each value is given in its string representation, as in MariaDB text protocol,
   *                    except boolean (BIT(1)) values that are represented as "1" or "0" strings
   * @param protocol    protocol
   * @return resultset
   */
  public static ResultSet createResultSet(String[] columnNames, ColumnType[] columnTypes,
      String[][] data,
      Protocol protocol) {
    int columnNameLength = columnNames.length;
    ColumnInformation[] columns = new ColumnInformation[columnNameLength];

    for (int i = 0; i < columnNameLength; i++) {
      columns[i] = ColumnInformation.create(columnNames[i], columnTypes[i]);
    }

    List<byte[]> rows = new ArrayList<byte[]>();

    for (String[] rowData : data) {
      assert rowData.length == columnNameLength;
      byte[][] rowBytes = new byte[rowData.length][];
      for (int i = 0; i < rowData.length; i++) {
        if (rowData[i] != null) {
          rowBytes[i] = rowData[i].getBytes();
        }
      }
      rows.add(StandardPacketInputStream.create(rowBytes, columnTypes));
    }
    return new SelectResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
  }

  public static SelectResultSet createEmptyResultSet() {
    return new SelectResultSet(INSERT_ID_COLUMNS, new ArrayList<byte[]>(), null,
        TYPE_SCROLL_SENSITIVE);
  }

  /**
   * Indicate if result-set is still streaming results from server.
   *
   * @return true if streaming is finished
   */
  public boolean isFullyLoaded() {
    //result-set is fully loaded when reaching EOF packet.
    return isEof;
  }

  private void fetchAllResults() throws IOException, SQLException {

    dataSize = 0;
    while (readNextValue()) {
      //fetch all results
    }
    dataFetchTime++;
  }

  /**
   * When protocol has a current Streaming result (this) fetch all to permit another query is
   * executing.
   *
   * @throws SQLException if any error occur
   */
  public void fetchRemaining() throws SQLException {
    if (!isEof) {
      lock.lock();
      try {
        lastRowPointer = -1;
        while (!isEof) {
          addStreamingValue();
        }

      } catch (SQLException queryException) {
        throw ExceptionMapper.getException(queryException, null, statement, false);
      } catch (IOException ioe) {
        throw handleIoException(ioe);
      } finally {
        lock.unlock();
      }
      dataFetchTime++;
    }
  }

  private SQLException handleIoException(IOException ioe) {
    return ExceptionMapper.getException(new SQLException("Server has closed the connection. "
        + "If result set contain huge amount of data, Server expects client to"
        + " read off the result set relatively fast. "
        + "In this case, please consider increasing net_wait_timeout session variable"
        + " / processing your result set faster (check Streaming result sets documentation for more information)",
        CONNECTION_EXCEPTION.getSqlState(), ioe), null, statement, false);
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
    if (resultSetScrollType == TYPE_FORWARD_ONLY) {
      dataSize = 0;
    }

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
      throw ExceptionMapper.get(errorPacket.getMessage(), errorPacket.getSqlState(),
          errorPacket.getErrorNumber(), null, false);
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
        if (callableResult) {
          serverStatus |= MORE_RESULTS_EXISTS;
        }

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
      if ((serverStatus & MORE_RESULTS_EXISTS) == 0) {
        protocol.removeActiveStreamingResult();
      }

      resetVariables();
      return false;
    }

    //this is a result-set row, save it
    if (dataSize + 1 >= data.length) {
      growDataArray();
    }
    data[dataSize++] = buf;
    return true;
  }

  /**
   * Get current row's raw bytes.
   *
   * @return row's raw bytes
   */
  protected byte[] getCurrentRowData() {
    return data[rowPointer];
  }

  /**
   * Update row's raw bytes. in case of row update, refresh the data. (format must correspond to
   * current resultset binary/text row encryption)
   *
   * @param rawData new row's raw data.
   */
  protected void updateRowData(byte[] rawData) {
    data[rowPointer] = rawData;
    row.resetRow(data[rowPointer]);
  }

  /**
   * Delete current data. Position cursor to the previous row.
   *
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
    if (dataSize + 1 >= data.length) {
      growDataArray();
    }
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
    if (newCapacity - MAX_ARRAY_SIZE > 0) {
      newCapacity = MAX_ARRAY_SIZE;
    }
    data = Arrays.copyOf(data, newCapacity);
  }

  /**
   * Connection.abort() has been called, abort result-set.
   *
   * @throws SQLException exception
   */
  public void abort() throws SQLException {
    isClosed = true;
    resetVariables();

    //keep garbage easy
    for (int i = 0; i < data.length; i++) {
      data[i] = null;
    }

    if (statement != null) {
      statement.checkCloseOnCompletion(this);
      statement = null;
    }
  }

  /**
   * Close resultSet.
   */
  public void close() throws SQLException {
    isClosed = true;
    if (!isEof) {
      lock.lock();
      try {
        while (!isEof) {
          dataSize = 0; //to avoid storing data
          readNextValue();
        }

      } catch (SQLException queryException) {
        throw ExceptionMapper.getException(queryException, null, this.statement, false);
      } catch (IOException ioe) {
        throw handleIoException(ioe);
      } finally {
        resetVariables();
        lock.unlock();
      }
    }
    resetVariables();

    //keep garbage easy
    for (int i = 0; i < data.length; i++) {
      data[i] = null;
    }

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
    if (isClosed) {
      throw new SQLException("Operation not permit on a closed resultSet", "HY000");
    }
    if (rowPointer < dataSize - 1) {
      rowPointer++;
      return true;
    } else {
      if (streaming && !isEof) {
        lock.lock();
        try {
          if (!isEof) {
            nextStreamingValue();
          }
        } catch (IOException ioe) {
          throw handleIoException(ioe);
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
    row.setPosition(position - 1);
  }


  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (this.statement == null) {
      return null;
    }
    return this.statement.getWarnings();
  }

  @Override
  public void clearWarnings() {
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
        lock.lock();
        try {
          //this time, fetch is added even for streaming forward type only to keep current pointer row.
          if (!isEof) {
            addStreamingValue();
          }
        } catch (IOException ioe) {
          throw handleIoException(ioe);
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
      lock.lock();
      try {
        if (!isEof) {
          addStreamingValue();
        }
      } catch (IOException ioe) {
        throw handleIoException(ioe);
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
    fetchRemaining();
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
    fetchRemaining();
    rowPointer = dataSize - 1;
    return dataSize > 0;
  }

  @Override
  public int getRow() throws SQLException {
    checkClose();
    if (streaming && resultSetScrollType == TYPE_FORWARD_ONLY) {
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
    fetchRemaining();

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
    if (newPos <= -1) {
      rowPointer = -1;
      return false;
    } else if (newPos >= dataSize) {
      rowPointer = dataSize;
      return false;
    } else {
      rowPointer = newPos;
      return true;
    }
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
  public int getFetchDirection() {
    return FETCH_UNKNOWN;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction == FETCH_REVERSE) {
      throw new SQLException(
          "Invalid operation. Allowed direction are ResultSet.FETCH_FORWARD and ResultSet.FETCH_UNKNOWN");
    }
  }

  @Override
  public int getFetchSize() {
    return this.fetchSize;
  }

  @Override
  public void setFetchSize(int fetchSize) throws SQLException {
    if (streaming && fetchSize == 0) {
      lock.lock();
      try {
        //fetch all results
        while (!isEof) {
          addStreamingValue();
        }
      } catch (IOException ioe) {
        throw handleIoException(ioe);
      } finally {
        lock.unlock();
      }

      streaming = dataFetchTime == 1;
    }
    this.fetchSize = fetchSize;
  }

  @Override
  public int getType() {
    return resultSetScrollType;
  }

  @Override
  public int getConcurrency() {
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
  public boolean wasNull() {
    return row.wasNull();
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
    if (row.lastValueWasNull()) {
      return null;
    }
    return new ByteArrayInputStream(
        new String(row.buf, row.pos, row.getLengthMaxFieldSize(), StandardCharsets.UTF_8)
            .getBytes());
  }

  /**
   * {inheritDoc}.
   */
  public String getString(int columnIndex) throws SQLException {
    checkObjectRange(columnIndex);
    return row.getInternalString(columnsInformation[columnIndex - 1], null, timeZone);
  }

  /**
   * {inheritDoc}.
   */
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  private String zeroFillingIfNeeded(String value, ColumnInformation columnInformation) {
    if (columnInformation.isZeroFill()) {
      StringBuilder zeroAppendStr = new StringBuilder();
      long zeroToAdd = columnInformation.getDisplaySize() - value.length();
      while (zeroToAdd-- > 0) {
        zeroAppendStr.append("0");
      }
      return zeroAppendStr.append(value).toString();
    }
    return value;
  }

  /**
   * {inheritDoc}.
   */
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    checkObjectRange(columnIndex);
    if (row.lastValueWasNull()) {
      return null;
    }
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
    return row.getInternalInt(columnsInformation[columnIndex - 1]);
  }

  /**
   * {inheritDoc}.
   */
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
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
    return row.getInternalLong(columnsInformation[columnIndex - 1]);
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
    return row.getInternalFloat(columnsInformation[columnIndex - 1]);
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
    return row.getInternalDouble(columnsInformation[columnIndex - 1]);
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
    return row.getInternalBigDecimal(columnsInformation[columnIndex - 1]);
  }

  /**
   * {inheritDoc}.
   */
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    checkObjectRange(columnIndex);
    return row.getInternalBigDecimal(columnsInformation[columnIndex - 1]);
  }

  /**
   * {inheritDoc}.
   */
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
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
    if (row.lastValueWasNull()) {
      return null;
    }
    byte[] data = new byte[row.getLengthMaxFieldSize()];
    System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
    return data;
  }

  /**
   * {inheritDoc}.
   */
  public Date getDate(int columnIndex) throws SQLException {
    checkObjectRange(columnIndex);
    return row.getInternalDate(columnsInformation[columnIndex - 1], null, timeZone);
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
    return row.getInternalDate(columnsInformation[columnIndex - 1], cal, timeZone);
  }

  /**
   * {inheritDoc}.
   */
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return getDate(findColumn(columnLabel), cal);
  }

  /**
   * {inheritDoc}.
   */
  public Time getTime(int columnIndex) throws SQLException {
    checkObjectRange(columnIndex);
    return row.getInternalTime(columnsInformation[columnIndex - 1], null, timeZone);
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
    return row.getInternalTime(columnsInformation[columnIndex - 1], cal, timeZone);
  }

  /**
   * {inheritDoc}.
   */
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getTime(findColumn(columnLabel), cal);
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
    return row.getInternalTimestamp(columnsInformation[columnIndex - 1], cal, timeZone);
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
    return row.getInternalTimestamp(columnsInformation[columnIndex - 1], null, timeZone);
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
    if (row.lastValueWasNull()) {
      return null;
    }
    return new ByteArrayInputStream(
        new String(row.buf, row.pos, row.getLengthMaxFieldSize(), StandardCharsets.UTF_8)
            .getBytes());
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
  public ResultSetMetaData getMetaData() {
    return new MariaDbResultSetMetaData(columnsInformation, options, returnTableAlias);
  }

  /**
   * {inheritDoc}.
   */
  public Object getObject(int columnIndex) throws SQLException {
    checkObjectRange(columnIndex);
    return row.getInternalObject(columnsInformation[columnIndex - 1], timeZone);
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
    if (type == null) {
      throw new SQLException("Class type cannot be null");
    }
    checkObjectRange(columnIndex);
    if (row.lastValueWasNull()) {
      return null;
    }
    ColumnInformation col = columnsInformation[columnIndex - 1];

    if (type.equals(String.class)) {
      return (T) row.getInternalString(col, null, timeZone);

    } else if (type.equals(Integer.class)) {
      return (T) (Integer) row.getInternalInt(col);

    } else if (type.equals(Long.class)) {
      return (T) (Long) row.getInternalLong(col);

    } else if (type.equals(Short.class)) {
      return (T) (Short) row.getInternalShort(col);

    } else if (type.equals(Double.class)) {
      return (T) (Double) row.getInternalDouble(col);

    } else if (type.equals(Float.class)) {
      return (T) (Float) row.getInternalFloat(col);

    } else if (type.equals(Byte.class)) {
      return (T) (Byte) row.getInternalByte(col);

    } else if (type.equals(byte[].class)) {
      byte[] data = new byte[row.getLengthMaxFieldSize()];
      System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
      return (T) data;

    } else if (type.equals(Date.class)) {
      return (T) row.getInternalDate(col, null, timeZone);

    } else if (type.equals(Time.class)) {
      return (T) row.getInternalTime(col, null, timeZone);

    } else if (type.equals(Timestamp.class) || type.equals(java.util.Date.class)) {
      return (T) row.getInternalTimestamp(col, null, timeZone);

    } else if (type.equals(Boolean.class)) {
      return (T) (Boolean) row.getInternalBoolean(col);

    } else if (type.equals(Calendar.class)) {
      Calendar calendar = Calendar.getInstance(timeZone);
      Timestamp timestamp = row.getInternalTimestamp(col, null, timeZone);
      if (timestamp == null) {
        return null;
      }
      calendar.setTimeInMillis(timestamp.getTime());
      return type.cast(calendar);

    } else if (type.equals(Clob.class) || type.equals(NClob.class)) {
      return (T) new MariaDbClob(row.buf, row.pos, row.getLengthMaxFieldSize());

    } else if (type.equals(InputStream.class)) {
      return (T) new ByteArrayInputStream(row.buf, row.pos, row.getLengthMaxFieldSize());

    } else if (type.equals(Reader.class)) {
      String value = row.getInternalString(col, null, timeZone);
      if (value == null) {
        return null;
      }
      return (T) new StringReader(value);

    } else if (type.equals(BigDecimal.class)) {
      return (T) row.getInternalBigDecimal(col);

    } else if (type.equals(BigInteger.class)) {
      return (T) row.getInternalBigInteger(col);
    } else if (type.equals(BigDecimal.class)) {
      return (T) row.getInternalBigDecimal(col);
    }
    throw ExceptionMapper
        .getFeatureNotSupportedException("Type class '" + type.getName() + "' is not supported");

  }

  @SuppressWarnings("unchecked")
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return type.cast(getObject(findColumn(columnLabel), type));
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
    String value = row.getInternalString(columnsInformation[columnIndex - 1], null, timeZone);
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
    if (row.lastValueWasNull()) {
      return null;
    }
    return new MariaDbBlob(row.buf, row.pos, row.length);
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
    if (row.lastValueWasNull()) {
      return null;
    }
    return new MariaDbClob(row.buf, row.pos, row.length);
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
    if (row.lastValueWasNull()) {
      return null;
    }
    try {
      return new URL(row.getInternalString(columnsInformation[columnIndex - 1], null, timeZone));
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
    if (row.lastValueWasNull()) {
      return null;
    }
    return new MariaDbClob(row.buf, row.pos, row.length);
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
    return row.getInternalBoolean(columnsInformation[index - 1]);
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
    return row.getInternalByte(columnsInformation[index - 1]);
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
    return row.getInternalShort(columnsInformation[index - 1]);
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
    throw ExceptionMapper
        .getFeatureNotSupportedException("Detecting row updates are not supported");
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
    throw ExceptionMapper.getFeatureNotSupportedException(
        "insertRow are not supported when using ResultSet.CONCUR_READ_ONLY");
  }

  /**
   * {inheritDoc}.
   */
  public void deleteRow() throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(
        "deleteRow are not supported when using ResultSet.CONCUR_READ_ONLY");
  }

  /**
   * {inheritDoc}.
   */
  public void refreshRow() throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(
        "refreshRow are not supported when using ResultSet.CONCUR_READ_ONLY");
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
  public void updateAsciiStream(int columnIndex, InputStream inputStream, int length)
      throws SQLException {
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
  public void updateAsciiStream(String columnLabel, InputStream value, int length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateAsciiStream(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateAsciiStream(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
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
  public void updateBinaryStream(int columnIndex, InputStream inputStream, int length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateBinaryStream(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateBinaryStream(String columnLabel, InputStream value, int length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateBinaryStream(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
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
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateCharacterStream(int columnIndex, Reader value, long length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
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
  public void updateObject(String columnLabel, Object value, int scaleOrLength)
      throws SQLException {
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
    throw ExceptionMapper.getFeatureNotSupportedException(
        "updateRow are not supported when using ResultSet.CONCUR_READ_ONLY");
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
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
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
  public void updateNCharacterStream(int columnIndex, Reader value, long length)
      throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException(NOT_UPDATABLE_ERROR);
  }

  /**
   * {inheritDoc}.
   */
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
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
  public int getHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
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

  private void rangeCheck(Object className, long minValue, long maxValue, long value,
      ColumnInformation columnInfo) throws SQLException {
    if (value < minValue || value > maxValue) {
      throw new SQLException(
          "Out of range value for column '" + columnInfo.getName() + "' : value " + value
              + " is not in "
              + className + " range", "22003", 1264);
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

  public boolean isBinaryEncoded() {
    return row.isBinaryEncoded();
  }
}
