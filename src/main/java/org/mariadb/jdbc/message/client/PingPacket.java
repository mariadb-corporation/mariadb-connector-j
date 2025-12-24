// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import java.io.IOException;

import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.ClientMessage;
import static org.mariadb.jdbc.message.client.CommandConstants.COM_PING;

/** Ping packet see COM_PING (https://mariadb.com/kb/en/com_ping/) */
public final class PingPacket implements ClientMessage {

  /** default instance */
  public static final PingPacket INSTANCE = new PingPacket();

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(COM_PING);
    writer.flush();
    return 1;
  }
}
