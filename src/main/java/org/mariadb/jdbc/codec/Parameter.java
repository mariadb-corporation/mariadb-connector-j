// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

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
        public int getBinaryEncodeType() {
          return DataType.VARCHAR.get();
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
    codec.encodeBinary(encoder, context, this.value, this.cal, length);
  }

  public void encodeLongData(PacketWriter encoder, Context context)
      throws IOException, SQLException {
    codec.encodeLongData(encoder, context, this.value, length);
  }

  public byte[] encodeData(Context context) throws IOException, SQLException {
    return codec.encodeData(context, this.value, length);
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
}
