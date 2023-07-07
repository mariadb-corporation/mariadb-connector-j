// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import java.io.IOException;

public final class ChangeDbPacket implements RedoableClientMessage {

  private final String database;

  public ChangeDbPacket(String database) {
    this.database = database;
  }

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x02);
    writer.writeString(this.database);
    writer.flush();
    return 1;
  }
}
