// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.plugin.Codec;

/** Unified mock for MariaDB parameter. */
public class FuzzParameter implements Parameter {
  private final Codec<?> codec;
  private final Object value;
  private final Calendar cal = Calendar.getInstance();

  public FuzzParameter(Codec<?> codec, Object value) {
    this.codec = codec;
    this.value = value;
  }

  @Override
  public void encodeText(Writer encoder, Context context) throws IOException, SQLException {
    ((Codec<Object>) codec).encodeText(encoder, context, value, cal, 0L);
  }

  @Override
  public int getApproximateTextProtocolLength() {
    return -1;
  }

  @Override
  public void encodeBinary(Writer encoder, Context context) throws IOException, SQLException {
    ((Codec<Object>) codec).encodeBinary(encoder, context, value, cal, 0L);
  }

  @Override
  public void encodeLongData(Writer encoder) throws IOException, SQLException {}

  @Override
  public byte[] encodeData() throws IOException, SQLException {
    return new byte[0];
  }

  @Override
  public boolean canEncodeLongData() {
    return false;
  }

  @Override
  public int getBinaryEncodeType() {
    return 0;
  }

  @Override
  public boolean isNull() {
    return value == null;
  }

  @Override
  public String bestEffortStringValue(Context context) {
    return value == null ? null : value.toString();
  }
}
