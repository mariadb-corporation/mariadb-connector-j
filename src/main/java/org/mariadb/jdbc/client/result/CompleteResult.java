// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.result;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.util.ClosableLock;

/** Result-set that will retrieve all rows immediately before returning the result-set. */
public class CompleteResult extends Result {

  /** before first row position = initial position */
  protected static final int BEFORE_FIRST_POS = -1;

  /**
   * Constructor from exchanges
   *
   * @param stmt current statement
   * @param binaryProtocol does exchanges uses binary protocol
   * @param maxRows maximum number of rows
   * @param metadataList metadata
   * @param reader packet reader
   * @param context connection context
   * @param resultSetType result set type
   * @param closeOnCompletion close statement on completion
   * @param traceEnable network trace exchange possible
   * @throws IOException if Socket error occurs
   * @throws SQLException for all other kind of errors
   */
  @SuppressWarnings({"this-escape"})
  public CompleteResult(
      Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      ColumnDecoder[] metadataList,
      Reader reader,
      Context context,
      int resultSetType,
      boolean closeOnCompletion,
      boolean traceEnable)
      throws IOException, SQLException {

    super(
        stmt,
        binaryProtocol,
        maxRows,
        metadataList,
        reader,
        context,
        resultSetType,
        closeOnCompletion,
        traceEnable,
        false,
        0);
    this.data = new byte[10][];
    if (maxRows > 0) {
      this.data = new byte[10][];
      do {
        readNext(reader.readPacket(traceEnable));
      } while (!this.loaded && this.dataSize < maxRows);
      if (!this.loaded) skipRemaining();
    } else {
      // avoiding creating array of array since
      byte[] buf = reader.readPacket(traceEnable);
      if (buf[0] == (byte) 0xFF || (buf[0] == (byte) 0xFE && buf.length < 16777215)) {
        // no rows
        this.data = new byte[0][];
        readNext(buf);
      } else {
        byte[] buf2 = reader.readPacket(traceEnable);
        if (buf2[0] == (byte) 0xFF || (buf2[0] == (byte) 0xFE && buf2.length < 16777215)) {
          // one row
          this.data = new byte[1][];
          this.data[0] = buf;
          this.dataSize = 1;
          readNext(buf2);
        } else {
          // multiple rows
          this.data = new byte[10][];
          this.data[0] = buf;
          this.data[1] = buf2;
          this.dataSize = 2;
          do {
            readNext(reader.readPacket(traceEnable));
          } while (!this.loaded);
        }
      }
    }
  }

  private CompleteResult(ColumnDecoder[] metadataList, CompleteResult prev) {
    super(metadataList, prev);
  }

  /**
   * Specific constructor for internal build result-set, empty resultset, or generated key
   * result-set.
   *
   * @param metadataList metadata
   * @param data result-set data
   * @param context connection context
   * @param resultSetType result set type
   */
  public CompleteResult(
      ColumnDecoder[] metadataList, byte[][] data, Context context, int resultSetType) {
    super(metadataList, data, context, resultSetType);
  }

  /**
   * Specific constructor for generating generated key result-set.
   *
   * @param columnName column key
   * @param columnType column key type
   * @param data values
   * @param context connection context
   * @param flags column flags
   * @param resultSetType result set type
   * @return result-set
   */
  public static ResultSet createResultSet(
      String columnName,
      DataType columnType,
      String[][] data,
      Context context,
      int flags,
      int resultSetType) {
    return createResultSet(
        new String[] {columnName},
        new DataType[] {columnType},
        data,
        context,
        flags,
        resultSetType);
  }

  /**
   * Create a result set from given data. Useful for creating "fake" resultSets for
   * DatabaseMetaData, (one example is MariaDbDatabaseMetaData.getTypeInfo())
   *
   * @param columnNames - string array of column names
   * @param columnTypes - column types
   * @param data - each element of this array represents a complete row in the ResultSet. Each value
   *     is given in its string representation, as in MariaDB text protocol, except boolean (BIT(1))
   *     values that are represented as "1" or "0" strings
   * @param context connection context
   * @param flags column flags
   * @param resultSetType result set type
   * @return resultset
   */
  public static ResultSet createResultSet(
      String[] columnNames,
      DataType[] columnTypes,
      String[][] data,
      Context context,
      int flags,
      int resultSetType) {

    int columnNameLength = columnNames.length;
    ColumnDecoder[] columns = new ColumnDecoder[columnNameLength];

    for (int i = 0; i < columnNameLength; i++) {
      columns[i] = ColumnDecoder.create(columnNames[i], columnTypes[i], flags);
    }

    List<byte[]> rows = new ArrayList<>();
    for (String[] rowData : data) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      for (String rowDatum : rowData) {

        if (rowDatum != null) {
          byte[] bb = rowDatum.getBytes();
          int len = bb.length;
          if (len < 251) {
            baos.write((byte) len);
          } else {
            // assume length cannot be > 65536
            baos.write((byte) 0xfc);
            baos.write((byte) len);
            baos.write((byte) (len >>> 8));
          }
          baos.write(bb, 0, bb.length);
        } else {
          baos.write((byte) 0xfb);
        }
      }
      byte[] bb = baos.toByteArray();
      rows.add(bb);
    }
    return new CompleteResult(columns, rows.toArray(new byte[0][0]), context, resultSetType);
  }

  public CompleteResult newResultsetWithUseAliasAsName() {
    ColumnDecoder[] newMeta = new ColumnDecoder[metadataList.length];
    for (int i = 0; i < metadataList.length; i++) {
      newMeta[i] = metadataList[i].useAliasAsName();
    }
    return new CompleteResult(newMeta, this);
  }

  @Override
  public boolean next() throws SQLException {
    if (rowPointer < dataSize - 1) {
      setRow(data[++rowPointer]);
      return true;
    } else {
      // all data are reads and pointer is after last
      setNullRowBuf();
      rowPointer = dataSize;
      return false;
    }
  }

  @Override
  public boolean streaming() {
    return false;
  }

  @Override
  public void fetchRemaining() {}

  @Override
  public void closeFromStmtClose(ClosableLock lock) {
    this.closed = true;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClose();
    if (rowPointer < dataSize) {
      // has remaining results
      return false;
    } else {

      // has read all data and pointer is after last result
      // so result would have to always be true,
      // but when result contain no row at all jdbc say that must return false
      return dataSize > 0;
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClose();
    return rowPointer == 0 && dataSize > 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClose();
    return rowPointer == dataSize - 1 && dataSize > 0;
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClose();
    rowPointer = BEFORE_FIRST_POS;
    setNullRowBuf();
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    setNullRowBuf();
    rowPointer = dataSize;
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();
    rowPointer = 0;
    if (dataSize == 0) {
      setNullRowBuf();
      return false;
    }
    setRow(data[rowPointer]);
    return true;
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    rowPointer = dataSize - 1;
    if (rowPointer == BEFORE_FIRST_POS) {
      setNullRowBuf();
      return false;
    }
    setRow(data[rowPointer]);
    return true;
  }

  @Override
  public int getRow() throws SQLException {
    checkClose();
    return rowPointer == dataSize ? 0 : rowPointer + 1;
  }

  @Override
  public boolean absolute(int idx) throws SQLException {
    checkClose();
    if (idx == 0 || idx > dataSize) {
      rowPointer = idx == 0 ? BEFORE_FIRST_POS : dataSize;
      setNullRowBuf();
      return false;
    }

    if (idx > 0) {
      rowPointer = idx - 1;
      setRow(data[rowPointer]);
      return true;
    } else {
      if (dataSize + idx >= 0) {
        // absolute position reverse from ending resultSet
        rowPointer = dataSize + idx;
        setRow(data[rowPointer]);
        return true;
      }
      rowPointer = BEFORE_FIRST_POS;
      setNullRowBuf();
      return false;
    }
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClose();
    int newPos = rowPointer + rows;
    if (newPos <= -1) {
      rowPointer = BEFORE_FIRST_POS;
      setNullRowBuf();
      return false;
    } else if (newPos >= dataSize) {
      rowPointer = dataSize;
      setNullRowBuf();
      return false;
    } else {
      rowPointer = newPos;
      setRow(data[rowPointer]);
      return true;
    }
  }

  @Override
  public boolean previous() throws SQLException {
    checkClose();
    if (rowPointer > BEFORE_FIRST_POS) {
      rowPointer--;
      if (rowPointer != BEFORE_FIRST_POS) {
        setRow(data[rowPointer]);
        return true;
      }
    }
    setNullRowBuf();
    return false;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClose();
    return super.getFetchSize();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkClose();
    super.setFetchSize(rows);
  }
}
