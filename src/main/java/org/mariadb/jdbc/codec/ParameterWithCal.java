// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;

public class ParameterWithCal<T> extends Parameter<T> {

  private final Calendar cal;

  public ParameterWithCal(Codec<T> codec, T value, Calendar cal) {
    super(codec, value);
    this.cal = cal;
  }

  @Override
  public void encodeText(PacketWriter encoder, Context context) throws IOException, SQLException {
    codec.encodeText(encoder, context, this.value, this.cal, length);
  }

  @Override
  public void encodeBinary(PacketWriter encoder) throws IOException, SQLException {
    codec.encodeBinary(encoder, this.value, this.cal, length);
  }
}
