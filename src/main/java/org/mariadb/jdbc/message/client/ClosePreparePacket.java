// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.ClientMessage;

/**
 * Client mysql COM_STMT_CLOSE packet COM_STMT_CLOSE packet. See
 * https://mariadb.com/kb/en/3-binary-protocol-prepared-statements-com_stmt_close/
 */
public final class ClosePreparePacket implements ClientMessage {

  private final int statementId;

  /**
   * Constructor for a prepare statement id
   *
   * @param statementId statement identifier
   */
  public ClosePreparePacket(int statementId) {
    this.statementId = statementId;
  }

  /** send packet to socket */
  @Override
  public int encode(Writer writer, Context context) throws IOException {
    assert statementId != 0;
    writer.initPacket();
    writer.writeByte(0x19);
    writer.writeInt(statementId);
    writer.flush();
    return 0;
  }
}
