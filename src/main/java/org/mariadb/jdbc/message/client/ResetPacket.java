// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.ClientMessage;

/** Reset packet COM_RESET_CONNECTION see https://mariadb.com/kb/en/com_reset_connection/ */
public final class ResetPacket implements ClientMessage {

  /** default instance */
  public static final ResetPacket INSTANCE = new ResetPacket();

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x1f);
    writer.flush();
    return 1;
  }
}
