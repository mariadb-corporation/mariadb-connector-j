// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.ClientMessage;

/**
 * ending connection packet COM_QUIT proper end of a connection. see
 * https://mariadb.com/kb/en/com_quit/
 */
public final class QuitPacket implements ClientMessage {

  /** default instance to encode packet */
  public static final QuitPacket INSTANCE = new QuitPacket();

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x01);
    writer.flush();
    return 0;
  }
}
