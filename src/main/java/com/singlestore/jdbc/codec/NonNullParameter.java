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

/**
 * Parameter wrapper for primitive, knowing that value cannot be null, permitting fast path for few
 * methods.
 *
 * @param <T> value type
 */
public class NonNullParameter<T> extends Parameter<T> {

  public NonNullParameter(Codec<T> codec, T value) {
    super(codec, value);
  }

  @Override
  public void encodeText(Writer encoder, Context context) throws IOException, SQLException {
    codec.encodeText(encoder, context, this.value, null, length);
  }

  @Override
  public boolean isNull() {
    return false;
  }

  @Override
  public int getApproximateTextProtocolLength() throws SQLException {
    return 0;
  }
}
