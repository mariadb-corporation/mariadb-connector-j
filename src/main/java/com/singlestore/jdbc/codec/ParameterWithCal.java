// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.codec;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.plugin.Codec;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;

public class ParameterWithCal<T> extends Parameter<T> {

  private final Calendar cal;

  public ParameterWithCal(Codec<T> codec, T value, Calendar cal) {
    super(codec, value);
    this.cal = cal;
  }

  @Override
  public void encodeText(Writer encoder, Context context) throws IOException, SQLException {
    codec.encodeText(encoder, context, this.value, this.cal, length);
  }

  @Override
  public void encodeBinary(Writer encoder) throws IOException, SQLException {
    codec.encodeBinary(encoder, this.value, this.cal, length);
  }
}
