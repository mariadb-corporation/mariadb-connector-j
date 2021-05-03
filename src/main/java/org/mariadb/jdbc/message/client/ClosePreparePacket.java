// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;

public final class ClosePreparePacket implements ClientMessage {

  private final int statementId;

  public ClosePreparePacket(int statementId) {
    this.statementId = statementId;
  }

  /**
   * COM_STMT_CLOSE packet. See
   * https://mariadb.com/kb/en/3-binary-protocol-prepared-statements-com_stmt_close/
   */
  @Override
  public int encode(PacketWriter writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x19);
    writer.writeInt(statementId);
    writer.flush();
    return 0;
  }
}
