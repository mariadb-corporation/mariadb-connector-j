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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;

public class Parameter<T> {
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static final Parameter<?> NULL_PARAMETER =
      new Parameter(null, null) {
        @Override
        public void encodeText(PacketWriter encoder, Context context) throws IOException {
          encoder.writeAscii("null");
        }

        @Override
        public DataType getBinaryEncodeType() {
          return DataType.VARCHAR;
        }

        @Override
        public boolean isNull() {
          return true;
        }
      };

  private final Codec<T> codec;
  private final T value;
  private final Calendar cal;
  private final Long length;

  public Parameter(Codec<T> codec, T value) {
    this.codec = codec;
    this.value = value;
    this.cal = null;
    this.length = null;
  }

  public Parameter(Codec<T> codec, T value, Long length) {
    this.codec = codec;
    this.value = value;
    this.cal = null;
    this.length = length;
  }

  public Parameter(Codec<T> codec, T value, Calendar cal) {
    this.codec = codec;
    this.value = value;
    this.cal = cal;
    this.length = null;
  }

  public void encodeText(PacketWriter encoder, Context context) throws IOException, SQLException {
    codec.encodeText(encoder, context, this.value, this.cal, length);
  }

  public void encodeBinary(PacketWriter encoder, Context context) throws IOException, SQLException {
    codec.encodeBinary(encoder, context, this.value, this.cal);
  }

  public void encodeLongData(PacketWriter encoder, Context context)
      throws IOException, SQLException {
    codec.encodeLongData(encoder, context, this.value, length);
  }

  public byte[] encodeLongDataReturning(PacketWriter encoder, Context context)
      throws IOException, SQLException {
    return codec.encodeLongDataReturning(encoder, context, this.value, length);
  }

  public Long getLength() {
    return length;
  }

  public boolean canEncodeLongData() {
    return codec.canEncodeLongData();
  }

  public DataType getBinaryEncodeType() {
    return codec.getBinaryEncodeType();
  }

  public boolean isNull() {
    return false;
  }

  @Override
  public String toString() {
    return "Parameter{codec=" + codec.className() + ", value=" + value + '}';
  }
}
