// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.codec;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.socket.impl.PacketWriter;
import org.mariadb.jdbc.plugin.Codec;

public class Parameter<T> implements org.mariadb.jdbc.client.util.Parameter {
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static final Parameter<?> NULL_PARAMETER =
      new Parameter(null, null) {
        @Override
        public int getBinaryEncodeType() {
          return DataType.VARCHAR.get();
        }

        @Override
        public boolean isNull() {
          return true;
        }
      };

  protected final Codec<T> codec;
  protected final T value;
  protected final Long length;

  public Parameter(Codec<T> codec, T value) {
    this.codec = codec;
    this.value = value;
    this.length = null;
  }

  public Parameter(Codec<T> codec, T value, Long length) {
    this.codec = codec;
    this.value = value;
    this.length = length;
  }

  public void encodeText(Writer encoder, Context context) throws IOException, SQLException {
    if (value == null) {
      encoder.writeAscii("null");
    } else {
      codec.encodeText(encoder, context, this.value, null, length);
    }
  }

  public void encodeBinary(Writer encoder) throws IOException, SQLException {
    codec.encodeBinary(encoder, this.value, null, length);
  }

  public void encodeLongData(Writer encoder) throws IOException, SQLException {
    codec.encodeLongData(encoder, this.value, length);
  }

  public byte[] encodeData() throws IOException, SQLException {
    return codec.encodeData(this.value, length);
  }

  public boolean canEncodeLongData() {
    return codec.canEncodeLongData();
  }

  public int getBinaryEncodeType() {
    return codec.getBinaryEncodeType();
  }

  public boolean isNull() {
    return value == null;
  }

  public String bestEffortStringValue(Context context) {
    if (isNull()) return "null";
    if (codec.canEncodeLongData()) {
      Type it = codec.getClass().getGenericInterfaces()[0];
      ParameterizedType parameterizedType = (ParameterizedType) it;
      Type typeParameter = parameterizedType.getActualTypeArguments()[0];
      return "<" + typeParameter + ">";
    }
    try {
      PacketWriter writer = new PacketWriter(null, 0, 0xffffff, null, null);
      codec.encodeText(writer, context, this.value, null, this.length);
      return new String(writer.buf(), 4, writer.pos() - 4, StandardCharsets.UTF_8);
    } catch (Throwable t) {
      return null;
    }
  }
}
