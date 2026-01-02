// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import static org.mariadb.jdbc.message.client.CommandConstants.COM_INIT_DB;

import java.io.IOException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;

/** change database. See https://mariadb.com/kb/en/com_init_db/ protocol */
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

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(COM_INIT_DB);
    writer.writeString(this.database);
    writer.flush();
    return 1;
  }
}
