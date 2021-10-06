// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import java.io.IOException;

public final class QueryPacket implements RedoableClientMessage {

  private final String sql;

  public QueryPacket(String sql) {
    this.sql = sql;
  }

  public int batchUpdateLength() {
    return 1;
  }

  @Override
  public int encode(PacketWriter writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x03);
    writer.writeString(this.sql);
    writer.flush();
    return 1;
  }

  public boolean isCommit() {
    return "COMMIT".equalsIgnoreCase(sql);
  }

  public String description() {
    return sql;
  }
}
