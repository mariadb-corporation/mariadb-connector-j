// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.*;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public abstract class RowDecoder {
  protected static final int NULL_LENGTH = -1;
  private static final CodecList codecList = CodecLoader.get();
  private final Configuration conf;
  protected ReadableByteBuf readBuf = new ReadableByteBuf(null, null, 0);
  protected ColumnDefinitionPacket[] columns;

  protected int length;
  protected int index;
  protected int columnCount;
  private Map<String, Integer> mapper = null;

  public RowDecoder(int columnCount, ColumnDefinitionPacket[] columns, Configuration conf) {
    this.columnCount = columnCount;
    this.columns = columns;
    this.conf = conf;
  }

  public void setRow(byte[] buf) {
    this.readBuf.buf(buf, buf == null ? 0 : buf.length).pos(0);
    index = -1;
  }

  public abstract void setPosition(int position);

  public abstract <T> T decode(Codec<T> codec, Calendar calendar) throws SQLException;

  @SuppressWarnings("unchecked")
  public <T> T getValue(int index, Class<T> type, Calendar calendar) throws SQLException {
    if (readBuf.buf() == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    if (index < 1 || index > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, columnCount));
    }

    setPosition(index - 1);

    if (wasNull()) {
      if (type.isPrimitive()) {
        throw new SQLException(
            String.format("Cannot return null for primitive %s", type.getName()));
      }
      return null;
    }

    ColumnDefinitionPacket column = columns[index - 1];
    // type generic, return "natural" java type
    if (Object.class.equals(type) || type == null) {
      Codec<T> defaultCodec = ((Codec<T>) column.getDefaultCodec(conf));
      return decode(defaultCodec, calendar);
    }

    for (Codec<?> codec : codecList.getCodecs()) {
      if (codec.canDecode(column, type)) {
        return decode((Codec<T>) codec, calendar);
      }
    }
    readBuf.skip(length);
    throw new SQLException(
        String.format("Type %s not supported type for %s type", type, column.getType().name()));
  }

  public abstract boolean wasNull();

  /**
   * Get value.
   *
   * @param index REAL index (0 = first)
   * @param codec codec
   * @return value
   * @throws SQLException if cannot decode value
   */
  public <T> T getValue(int index, Codec<T> codec, Calendar cal) throws SQLException {
    if (index < 1 || index > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, columnCount));
    }
    if (readBuf.buf() == null) {
      throw new SQLDataException("wrong row position", "22023");
    }

    setPosition(index - 1);
    if (length == NULL_LENGTH) {
      return null;
    }
    return decode(codec, cal);
  }

  public <T> T getValue(String label, Codec<T> codec, Calendar cal) throws SQLException {
    if (readBuf.buf() == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    return getValue(getIndex(label), codec, cal);
  }

  public int getIndex(String label) throws SQLException {
    if (label == null) throw new SQLException("null is not a valid label value");
    if (mapper == null) {
      mapper = new HashMap<>();
      for (int i = 0; i < columnCount; i++) {
        ColumnDefinitionPacket ci = columns[i];
        String columnAlias = ci.getColumnAlias();
        if (columnAlias != null) {
          columnAlias = columnAlias.toLowerCase(Locale.ROOT);
          mapper.putIfAbsent(columnAlias, i + 1);

          String tableAlias = ci.getTableAlias();
          if (tableAlias != null) {
            mapper.putIfAbsent(tableAlias.toLowerCase(Locale.ROOT) + "." + columnAlias, i + 1);
          } else {
            String table = ci.getTable();
            if (table != null) {
              mapper.putIfAbsent(table.toLowerCase(Locale.ROOT) + "." + columnAlias, i + 1);
            }
          }
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
