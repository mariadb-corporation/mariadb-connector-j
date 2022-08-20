// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.message.client;

import java.io.IOException;
import org.tidb.jdbc.client.Context;
import org.tidb.jdbc.client.socket.Writer;
import org.tidb.jdbc.message.ClientMessage;

/** Ping packet see COM_PING (https://mariadb.com/kb/en/com_ping/) */
public final class PingPacket implements ClientMessage {

  /** default instance */
  public static final PingPacket INSTANCE = new PingPacket();

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x0e);
    writer.flush();
    return 1;
  }
}
