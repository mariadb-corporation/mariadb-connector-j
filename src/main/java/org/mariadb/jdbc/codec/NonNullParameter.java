// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;

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
}
