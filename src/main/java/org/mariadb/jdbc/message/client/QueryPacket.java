// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;

/** Query client packet COM_QUERY see https://mariadb.com/kb/en/com_query/ */
public final class QueryPacket implements RedoableClientMessage {

  private final String sql;

  /**
   * Constructor
   *
   * @param sql sql command
   */
  public QueryPacket(String sql) {
    this.sql = sql;
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

  public String description() {
    return sql;
  }
}
