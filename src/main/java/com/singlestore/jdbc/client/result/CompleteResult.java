// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.result;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.Column;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class CompleteResult extends Result {

  protected static final int BEFORE_FIRST_POS = -1;

  public CompleteResult(
      Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      Column[] metadataList,
      com.singlestore.jdbc.client.socket.Reader reader,
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
        traceEnable);
    this.data = new byte[10][];
    if (maxRows > 0) {
      while (readNext() && dataSize < maxRows) {}
      if (!loaded) skipRemaining();
    } else {
      while (readNext()) {}
    }
    loaded = true;
  }

  public CompleteResult(ColumnDefinitionPacket[] metadataList, byte[][] data, Context context) {
    super(metadataList, data, context);
  }

  public static ResultSet createResultSet(
      String columnName, DataType columnType, String[][] data, Context context) {
    return createResultSet(new String[] {columnName}, new DataType[] {columnType}, data, context);
  }

  /**
   * Create a result set from given data. Useful for creating "fake" resultSets for
   * DatabaseMetaData, (one example is SingleStoreDatabaseMetaData.getTypeInfo())
   *
   * @param columnNames - string array of column names
   * @param columnTypes - column types
   * @param data - each element of this array represents a complete row in the ResultSet. Each value
   *     is given in its string representation, as in SingleStore text protocol, except boolean
   *     (BIT(1)) values that are represented as "1" or "0" strings
   * @param context connection context
   * @return resultset
   */
  public static ResultSet createResultSet(
      String[] columnNames, DataType[] columnTypes, String[][] data, Context context) {

    int columnNameLength = columnNames.length;
    ColumnDefinitionPacket[] columns = new ColumnDefinitionPacket[columnNameLength];

    for (int i = 0; i < columnNameLength; i++) {
      columns[i] = ColumnDefinitionPacket.create(columnNames[i], columnTypes[i]);
    }

    List<byte[]> rows = new ArrayList<>();
    for (String[] rowData : data) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      for (int i = 0; i < rowData.length; i++) {

        if (rowData[i] != null) {
          byte[] bb = rowData[i].getBytes();
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
    return new CompleteResult(columns, rows.toArray(new byte[0][0]), context);
  }

  @Override
  public boolean next() throws SQLException {
    if (rowPointer < dataSize - 1) {
      row.setRow(data[++rowPointer]);
      return true;
    } else {
      // all data are reads and pointer is after last
      row.setRow(null);
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
  public void closeFromStmtClose(ReentrantLock lock) {
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
      // so result would have to always to be true,
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
    row.setRow(null);
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    row.setRow(null);
    rowPointer = dataSize;
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();
    rowPointer = 0;
    if (dataSize == 0) {
      row.setRow(null);
      return false;
    }
    row.setRow(data[rowPointer]);
    return true;
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    rowPointer = dataSize - 1;
    if (rowPointer == BEFORE_FIRST_POS) {
      row.setRow(null);
      return false;
    }
    row.setRow(data[rowPointer]);
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
      row.setRow(null);
      return false;
    }

    if (idx > 0) {
      rowPointer = idx - 1;
      row.setRow(data[rowPointer]);
      return true;
    } else {
      if (dataSize + idx >= 0) {
        // absolute position reverse from ending resultSet
        rowPointer = dataSize + idx;
        row.setRow(data[rowPointer]);
        return true;
      }
      rowPointer = BEFORE_FIRST_POS;
      row.setRow(null);
      return false;
    }
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClose();
    int newPos = rowPointer + rows;
    if (newPos <= -1) {
      rowPointer = BEFORE_FIRST_POS;
      row.setRow(null);
      return false;
    } else if (newPos >= dataSize) {
      rowPointer = dataSize;
      row.setRow(null);
      return false;
    } else {
      rowPointer = newPos;
      row.setRow(data[rowPointer]);
      return true;
    }
  }

  @Override
  public boolean previous() throws SQLException {
    checkClose();
    if (rowPointer > BEFORE_FIRST_POS) {
      rowPointer--;
      if (rowPointer != BEFORE_FIRST_POS) {
        row.setRow(data[rowPointer]);
        return true;
      }
    }
    row.setRow(null);
    return false;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClose();
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkClose();
  }
}
