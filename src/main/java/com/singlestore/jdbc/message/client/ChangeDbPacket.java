// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import java.io.IOException;

public final class ChangeDbPacket implements RedoableClientMessage {

  private final String database;

  /**
   * Constructor to encode COM_INIT_DB packet
   *
   * @param database database
   */
  public ChangeDbPacket(String database) {
    this.database = database;
  }

  /**
   * COM_INIT_DB packet
   *
   * <p>int[1] 0x02 : COM_INIT_DB Header string[NUL] schema name
   */
  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x02);
    writer.writeString(this.database);
    writer.flush();
    return 1;
  }
}
