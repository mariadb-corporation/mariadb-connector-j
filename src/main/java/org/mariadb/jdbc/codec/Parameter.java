// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.plugin.Codec;

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

  public void encodeText(PacketWriter encoder, Context context) throws IOException, SQLException {
    codec.encodeText(encoder, context, this.value, null, length);
  }

  public void encodeBinary(PacketWriter encoder) throws IOException, SQLException {
    codec.encodeBinary(encoder, this.value, null, length);
  }

  public void encodeLongData(PacketWriter encoder) throws IOException, SQLException {
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
}
