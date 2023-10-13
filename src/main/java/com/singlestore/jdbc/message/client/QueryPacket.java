// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.message.ClientMessage;
import java.io.IOException;
import java.io.InputStream;

public final class QueryPacket implements RedoableClientMessage {

  private final String sql;
  private final InputStream localInfileInputStream;

  /**
   * Constructor
   *
   * @param sql sql command
   */
  public QueryPacket(String sql) {
    this.sql = sql;
    this.localInfileInputStream = null;
  }

  public QueryPacket(String sql, InputStream localInfileInputStream) {
    this.sql = sql;
    this.localInfileInputStream = localInfileInputStream;
  }

  public int batchUpdateLength() {
    return 1;
  }

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x03);
    writer.writeString(this.sql);
    writer.flush();
    return 1;
  }

  /**
   * Check that command is a COMMIT command
   *
   * @return true if a commit command
   */
  public boolean isCommit() {
    return "COMMIT".equalsIgnoreCase(sql);
  }

  public boolean validateLocalFileName(String fileName, Context context) {
    return ClientMessage.validateLocalFileName(sql, null, fileName, context);
  }

  public InputStream getLocalInfileInputStream() {
    return localInfileInputStream;
  }

  public String description() {
    return sql;
  }
}
