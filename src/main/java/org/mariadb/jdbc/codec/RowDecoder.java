/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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
 */

package org.mariadb.jdbc.codec;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.*;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public abstract class RowDecoder {
  protected static final int NULL_LENGTH = -1;

  private final Configuration conf;
  protected ReadableByteBuf buf;
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

  public void setRow(ReadableByteBuf buf) {
    if (buf == null) {
      this.buf = null;
    } else {
      this.buf = buf;
      this.buf.pos(0);
      index = -1;
    }
  }

  public abstract void setPosition(int position);

  public abstract <T> T decode(Codec<T> codec, Calendar calendar) throws SQLException;

  @SuppressWarnings("unchecked")
  public <T> T getValue(int index, Class<T> type, Calendar calendar) throws SQLException {
    if (buf == null) {
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
    if (Object.class == type || type == null) {
      Codec<T> defaultCodec = ((Codec<T>) column.getDefaultCodec(conf));
      return decode(defaultCodec, calendar);
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canDecode(column, type)) {
        return decode((Codec<T>) codec, calendar);
      }
    }
    buf.skip(length);
    throw new SQLException(
        String.format("Type %s not supported type for %s type", type, column.getType().name()));
  }

  public abstract boolean wasNull();

  public <T> T getValue(int index, Codec<T> codec) throws SQLException {
    return getValue(index, codec, null);
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
    if (index < 1 || index > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, columnCount));
    }
    if (buf == null) {
      throw new SQLDataException("wrong row position", "22023");
    }

    setPosition(index - 1);
    if (length == NULL_LENGTH) {
      return null;
    }
    return decode(codec, cal);
  }

  public <T> T getValue(String label, Codec<T> codec) throws SQLException {
    if (buf == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    return getValue(getIndex(label), codec);
  }

  public <T> T getValue(String label, Codec<T> codec, Calendar cal) throws SQLException {
    if (buf == null) {
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
