// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.codec.BinaryRowDecoder;
import org.mariadb.jdbc.codec.RowDecoder;
import org.mariadb.jdbc.codec.TextRowDecoder;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.codec.*;
import org.mariadb.jdbc.util.constants.ServerStatus;

public abstract class Result implements ResultSet, Completion {

  protected final int resultSetType;
  protected final ExceptionFactory exceptionFactory;
  protected final org.mariadb.jdbc.client.socket.Reader reader;
  protected final Context context;
  private final int maxIndex;
  private final boolean closeOnCompletion;
  protected final Column[] metadataList;
  protected final RowDecoder row;
  protected int dataSize = 0;
  protected byte[][] data;
  protected boolean loaded;
  protected boolean outputParameter;
  protected int rowPointer = -1;
  protected boolean closed;
  protected Statement statement;
  protected long maxRows;
  private boolean forceAlias;
  private final boolean traceEnable;

  public Result(
      org.mariadb.jdbc.Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      Column[] metadataList,
      org.mariadb.jdbc.client.socket.Reader reader,
      Context context,
      int resultSetType,
      boolean closeOnCompletion,
      boolean traceEnable) {
    this.maxRows = maxRows;
    this.statement = stmt;
    this.closeOnCompletion = closeOnCompletion;
    this.metadataList = metadataList;
    this.maxIndex = this.metadataList.length;
    this.reader = reader;
    this.exceptionFactory = context.getExceptionFactory();
    this.context = context;
    this.resultSetType = resultSetType;
    this.traceEnable = traceEnable;
    row =
        binaryProtocol
            ? new BinaryRowDecoder(this.maxIndex, metadataList, context.getConf())
            : new TextRowDecoder(this.maxIndex, metadataList, context.getConf());
  }

  public Result(ColumnDefinitionPacket[] metadataList, byte[][] data, Context context) {
    this.metadataList = metadataList;
    this.maxIndex = this.metadataList.length;
    this.reader = null;
    this.loaded = true;
    this.exceptionFactory = context.getExceptionFactory();
    this.context = context;
    this.data = data;
    this.dataSize = data.length;
    this.statement = null;
    this.resultSetType = TYPE_FORWARD_ONLY;
    this.closeOnCompletion = false;
    this.traceEnable = false;
    row = new TextRowDecoder(maxIndex, metadataList, context.getConf());
  }

  @SuppressWarnings("fallthrough")
  protected boolean readNext() throws SQLException, IOException {
    byte[] buf = reader.readPacket(false, traceEnable).buf();
    switch (buf[0]) {
      case (byte) 0xFF:
        loaded = true;
        ErrorPacket errorPacket =
            new ErrorPacket(new StandardReadableByteBuf(null, buf, buf.length), context);
        throw exceptionFactory.create(
            errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

      case (byte) 0xFE:
        if ((context.isEofDeprecated() && buf.length < 16777215)
            || (!context.isEofDeprecated() && buf.length < 8)) {
          ReadableByteBuf readBuf = new StandardReadableByteBuf(null, buf, buf.length);
          readBuf.skip(); // skip header
          int serverStatus;
          int warnings;

          if (!context.isEofDeprecated()) {
            // EOF_Packet
            warnings = readBuf.readUnsignedShort();
            serverStatus = readBuf.readUnsignedShort();
          } else {
            // OK_Packet with a 0xFE header
            readBuf.skip(readBuf.readLengthNotNull()); // skip update count
            readBuf.skip(readBuf.readLengthNotNull()); // skip insert id
            serverStatus = readBuf.readUnsignedShort();
            warnings = readBuf.readUnsignedShort();
          }
          outputParameter = (serverStatus & ServerStatus.PS_OUT_PARAMETERS) != 0;
          context.setServerStatus(serverStatus);
          context.setWarning(warnings);
          loaded = true;
          return false;
        }

        // continue reading rows

      default:
        if (dataSize + 1 > data.length) {
          growDataArray();
        }
        data[dataSize++] = buf;
    }
    return true;
  }

  @SuppressWarnings("fallthrough")
  protected void skipRemaining() throws SQLException, IOException {
    while (true) {
      ReadableByteBuf buf = reader.readPacket(true, traceEnable);
      switch (buf.getUnsignedByte()) {
        case 0xFF:
          loaded = true;
          ErrorPacket errorPacket = new ErrorPacket(buf, context);
          throw exceptionFactory.create(
              errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

        case 0xFE:
          if ((context.isEofDeprecated() && buf.readableBytes() < 0xffffff)
              || (!context.isEofDeprecated() && buf.readableBytes() < 8)) {

            buf.skip(); // skip header
            int serverStatus;
            int warnings;

            if (!context.isEofDeprecated()) {
              // EOF_Packet
              warnings = buf.readUnsignedShort();
              serverStatus = buf.readUnsignedShort();
            } else {
              // OK_Packet with a 0xFE header
              buf.skip(buf.readLengthNotNull()); // skip update count
              buf.skip(buf.readLengthNotNull()); // skip insert id
              serverStatus = buf.readUnsignedShort();
              warnings = buf.readUnsignedShort();
            }
            outputParameter = (serverStatus & ServerStatus.PS_OUT_PARAMETERS) != 0;
            context.setServerStatus(serverStatus);
            context.setWarning(warnings);
            loaded = true;
            return;
          }
      }
    }
  }

  /** Grow data array. */
  private void growDataArray() {
    int newCapacity = data.length + (data.length >> 1);
    data = Arrays.copyOf(data, newCapacity);
  }

  @Override
  public abstract boolean next() throws SQLException;

  public abstract boolean streaming();

  public abstract void fetchRemaining() throws SQLException;

  public boolean loaded() {
    return loaded;
  }

  public boolean isOutputParameter() {
    return outputParameter;
  }

  @Override
  public void close() throws SQLException {
    if (!loaded) {
      try {
        skipRemaining();
      } catch (IOException ioe) {
        throw exceptionFactory.create("Error while streaming resultSet data", "08000", ioe);
      }
    }
    this.closed = true;
    if (closeOnCompletion) {
      statement.close();
    }
  }

  public void closeFromStmtClose(ReentrantLock lock) throws SQLException {
    lock.lock();
    try {
      this.fetchRemaining();
      this.closed = true;
    } finally {
      lock.unlock();
    }
  }

  public void abort() {
    this.closed = true;
  }

  protected byte[] getCurrentRowData() {
    return data[0];
  }

  protected void addRowData(byte[] buf) {
    if (dataSize + 1 > data.length) {
      growDataArray();
    }
    data[dataSize++] = buf;
  }

  protected void updateRowData(byte[] rawData) {
    data[rowPointer] = rawData;
    row.setRow(rawData);
  }

  @Override
  public boolean wasNull() {
    return row.wasNull();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, StringCodec.INSTANCE, null);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return row.getBooleanValue(columnIndex);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return row.getByteValue(columnIndex);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return row.getShortValue(columnIndex);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return row.getIntValue(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return row.getLongValue(columnIndex);
  }

  public BigInteger getBigInteger(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, BigIntegerCodec.INSTANCE, null);
  }

  public BigInteger getBigInteger(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, BigIntegerCodec.INSTANCE, null);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return row.getFloatValue(columnIndex);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return row.getDoubleValue(columnIndex);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    BigDecimal d = row.getValue(columnIndex, BigDecimalCodec.INSTANCE, null);
    if (d == null) return null;
    return d.setScale(scale, RoundingMode.HALF_DOWN);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, ByteArrayCodec.INSTANCE, null);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, DateCodec.INSTANCE, null);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, TimeCodec.INSTANCE, null);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, TimestampCodec.INSTANCE, null);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, StreamCodec.INSTANCE, null);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, StreamCodec.INSTANCE, null);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, StreamCodec.INSTANCE, null);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, StringCodec.INSTANCE, null);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return row.getBooleanValue(row.getIndex(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return row.getByteValue(row.getIndex(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return row.getShortValue(row.getIndex(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return row.getIntValue(row.getIndex(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return row.getLongValue(row.getIndex(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return row.getFloatValue(row.getIndex(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return row.getDoubleValue(row.getIndex(columnLabel));
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    BigDecimal d = row.getValue(columnLabel, BigDecimalCodec.INSTANCE, null);
    if (d == null) return null;
    return d.setScale(scale, RoundingMode.HALF_DOWN);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, ByteArrayCodec.INSTANCE, null);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, DateCodec.INSTANCE, null);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, TimeCodec.INSTANCE, null);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, TimestampCodec.INSTANCE, null);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, StreamCodec.INSTANCE, null);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, StreamCodec.INSTANCE, null);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, StreamCodec.INSTANCE, null);
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
  public String getCursorName() throws SQLException {
    throw exceptionFactory.notSupported("Cursors are not supported");
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return new ResultSetMetaData(exceptionFactory, metadataList, context.getConf(), forceAlias);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (columnIndex < 1 || columnIndex > maxIndex) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", columnIndex, maxIndex));
    }
    Codec<?> defaultCodec = metadataList[columnIndex - 1].getDefaultCodec(context.getConf());
    return row.getValue(columnIndex, defaultCodec, null);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(row.getIndex(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return row.getIndex(columnLabel);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, ReaderCodec.INSTANCE, null);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, ReaderCodec.INSTANCE, null);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, BigDecimalCodec.INSTANCE, null);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, BigDecimalCodec.INSTANCE, null);
  }

  protected void checkClose() throws SQLException {
    if (closed) {
      throw exceptionFactory.create("Operation not permit on a closed resultSet", "HY000");
    }
  }

  protected void checkNotForwardOnly() throws SQLException {
    if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
      throw exceptionFactory.create("Operation not permit on TYPE_FORWARD_ONLY resultSet", "HY000");
    }
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClose();
    return rowPointer == -1 && dataSize > 0;
  }

  @Override
  public abstract boolean isAfterLast() throws SQLException;

  @Override
  public abstract boolean isFirst() throws SQLException;

  @Override
  public abstract boolean isLast() throws SQLException;

  @Override
  public abstract void beforeFirst() throws SQLException;

  @Override
  public abstract void afterLast() throws SQLException;

  @Override
  public abstract boolean first() throws SQLException;

  @Override
  public abstract boolean last() throws SQLException;

  @Override
  public abstract int getRow() throws SQLException;

  @Override
  public abstract boolean absolute(int row) throws SQLException;

  @Override
  public abstract boolean relative(int rows) throws SQLException;

  @Override
  public abstract boolean previous() throws SQLException;

  @Override
  public int getFetchDirection() {
    return FETCH_UNKNOWN;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction == FETCH_REVERSE) {
      throw exceptionFactory.create(
          "Invalid operation. Allowed direction are ResultSet.FETCH_FORWARD and ResultSet.FETCH_UNKNOWN");
    }
  }

  @Override
  public int getType() {
    return resultSetType;
  }

  @Override
  public int getConcurrency() {
    return CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void insertRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public Statement getStatement() {
    return statement;
  }

  public void setStatement(Statement stmt) {
    statement = stmt;
  }

  public void useAliasAsName() {
    for (Column packet : metadataList) {
      packet.useAliasAsName();
    }
    forceAlias = true;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    if (map == null || map.isEmpty()) {
      return getObject(columnIndex);
    }
    throw exceptionFactory.notSupported(
        "Method ResultSet.getObject(int columnIndex, Map<String, Class<?>> map) not supported for non empty map");
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getRef not supported");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, BlobCodec.INSTANCE, null);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, ClobCodec.INSTANCE, null);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getArray not supported");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    if (map == null || map.isEmpty()) {
      return getObject(columnLabel);
    }
    throw exceptionFactory.notSupported(
        "Method ResultSet.getObject(String columnLabel, Map<String, Class<?>> map) not supported");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getRef not supported");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, BlobCodec.INSTANCE, null);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, ClobCodec.INSTANCE, null);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getArray not supported");
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return row.getValue(columnIndex, DateCodec.INSTANCE, cal);
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return row.getValue(columnLabel, DateCodec.INSTANCE, cal);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return row.getValue(columnIndex, TimeCodec.INSTANCE, cal);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return row.getValue(columnLabel, TimeCodec.INSTANCE, cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return row.getValue(columnIndex, TimestampCodec.INSTANCE, cal);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return row.getValue(columnLabel, TimestampCodec.INSTANCE, cal);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    String s = row.getValue(columnIndex, StringCodec.INSTANCE, null);
    if (s == null) return null;
    try {
      return new URL(s);
    } catch (MalformedURLException e) {
      throw exceptionFactory.create(String.format("Could not parse '%s' as URL", s));
    }
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return getURL(row.getIndex(columnLabel));
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.updateRef not supported");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.updateRef not supported");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw exceptionFactory.notSupported("Array are not supported");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw exceptionFactory.notSupported("Array are not supported");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public int getHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return (NClob) row.getValue(columnIndex, ClobCodec.INSTANCE, null);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return (NClob) row.getValue(columnLabel, ClobCodec.INSTANCE, null);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getSQLXML not supported");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getSQLXML not supported");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.updateSQLXML not supported");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.updateSQLXML not supported");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, StringCodec.INSTANCE, null);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, StringCodec.INSTANCE, null);
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return row.getValue(columnIndex, ReaderCodec.INSTANCE, null);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return row.getValue(columnLabel, ReaderCodec.INSTANCE, null);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return row.getValue(columnIndex, type, null);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(row.getIndex(columnLabel), type);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException("The receiver is not a wrapper for " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }
}
