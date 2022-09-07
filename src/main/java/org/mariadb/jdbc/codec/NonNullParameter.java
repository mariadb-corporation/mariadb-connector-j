// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;

public class NonNullParameter<T> extends Parameter {

  public NonNullParameter(Codec<T> codec, T value) {
    super(codec, value);
  }

  public NonNullParameter(Codec<T> codec, T value, Long length) {
    super(codec, value, length);
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
