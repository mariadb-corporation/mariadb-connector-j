// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;

public final class PingPacket implements ClientMessage {

  public static final PingPacket INSTANCE = new PingPacket();

  @Override
  public int encode(PacketWriter writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x0e);
    writer.flush();
    return 0;
  }
}
