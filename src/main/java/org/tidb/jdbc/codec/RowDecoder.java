// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.codec;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.*;
import org.tidb.jdbc.Configuration;
import org.tidb.jdbc.client.Column;
import org.tidb.jdbc.client.ReadableByteBuf;
import org.tidb.jdbc.client.impl.StandardReadableByteBuf;
import org.tidb.jdbc.plugin.Codec;

public abstract class RowDecoder {
  protected static final int NULL_LENGTH = -1;
  private final Configuration conf;
  protected final ReadableByteBuf readBuf = new StandardReadableByteBuf(null, 0);
  protected final Column[] columns;

  protected int length;
  protected int index;
  protected final int columnCount;
  private Map<String, Integer> mapper = null;

  public RowDecoder(int columnCount, Column[] columns, Configuration conf) {
    this.columnCount = columnCount;
    this.columns = columns;
    this.conf = conf;
  }

  public void setRow(byte[] buf) {
    this.readBuf.buf(buf, buf == null ? 0 : buf.length, 0);
    index = -1;
  }

  public abstract void setPosition(int position);

  public abstract <T> T decode(Codec<T> codec, Calendar calendar) throws SQLException;

  public abstract byte decodeByte() throws SQLException;

  public abstract boolean decodeBoolean() throws SQLException;

  public abstract short decodeShort() throws SQLException;

  public abstract int decodeInt() throws SQLException;

  public abstract long decodeLong() throws SQLException;

  public abstract float decodeFloat() throws SQLException;

  public abstract double decodeDouble() throws SQLException;

  @SuppressWarnings("unchecked")
  public <T> T getValue(int index, Class<T> type, Calendar calendar) throws SQLException {
    checkIndexAndSetPosition(index);

    if (wasNull()) {
      if (type.isPrimitive()) {
        throw new SQLException(
            String.format("Cannot return null for primitive %s", type.getName()));
      }
      return null;
    }

    Column column = columns[index - 1];
    // type generic, return "natural" java type
    if (Object.class.equals(type) || type == null) {
      Codec<T> defaultCodec = ((Codec<T>) column.getDefaultCodec(conf));
      return decode(defaultCodec, calendar);
    }

    for (Codec<?> codec : conf.codecs()) {
      if (codec.canDecode(column, type)) {
        return decode((Codec<T>) codec, calendar);
      }
    }
    readBuf.skip(length);
    throw new SQLException(
        String.format("Type %s not supported type for %s type", type, column.getType().name()));
  }

  public abstract boolean wasNull();

  private void checkIndexAndSetPosition(int index) throws SQLException {
    if (index < 1 || index > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, columnCount));
    }
    if (readBuf.buf() == null) {
      throw new SQLDataException("wrong row position", "22023");
    }

    setPosition(index - 1);
  }

  /**
   * Get value.
   *
   * @param index REAL index (0 = first)
   * @param codec codec
   * @return value
   * @throws SQLException if cannot decode value
   */
  public <T> T getValue(int index, Codec<T> codec, Calendar cal) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return null;
    }
    return decode(codec, cal);
  }

  public byte getByteValue(int index) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return 0;
    }
    return decodeByte();
  }

  public boolean getBooleanValue(int index) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return false;
    }
    return decodeBoolean();
  }

  public short getShortValue(int index) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return 0;
    }
    return decodeShort();
  }

  public int getIntValue(int index) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return 0;
    }
    return decodeInt();
  }

  public long getLongValue(int index) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return 0L;
    }
    return decodeLong();
  }

  public float getFloatValue(int index) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return 0F;
    }
    return decodeFloat();
  }

  public double getDoubleValue(int index) throws SQLException {
    checkIndexAndSetPosition(index);
    if (length == NULL_LENGTH) {
      return 0D;
    }
    return decodeDouble();
  }

  public <T> T getValue(String label, Codec<T> codec, Calendar cal) throws SQLException {
    return getValue(getIndex(label), codec, cal);
  }

  public int getIndex(String label) throws SQLException {
    if (label == null) throw new SQLException("null is not a valid label value");
    if (mapper == null) {
      mapper = new HashMap<>();
      for (int i = 0; i < columnCount; i++) {
        Column ci = columns[i];
        String columnAlias = ci.getColumnAlias();
        if (columnAlias != null) {
          columnAlias = columnAlias.toLowerCase(Locale.ROOT);
          mapper.putIfAbsent(columnAlias, i + 1);
          String tableAlias = ci.getTableAlias();
          String tableLabel = tableAlias != null ? tableAlias : ci.getTable();
          mapper.putIfAbsent(tableLabel.toLowerCase(Locale.ROOT) + "." + columnAlias, i + 1);
        }
      }
    }
    Integer ind = mapper.get(label.toLowerCase(Locale.ROOT));
    if (ind == null) {
      String keys = Arrays.toString(mapper.keySet().toArray(new String[0]));
      throw new SQLException(String.format("Unknown label '%s'. Possible value %s", label, keys));
    }
    return ind;
  }
}
