// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.message.ClientMessage;
import java.io.IOException;

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

  /**
   * COM_STMT_CLOSE packet. See
   * https://mariadb.com/kb/en/3-binary-protocol-prepared-statements-com_stmt_close/
   */
  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(0x19);
    writer.writeInt(statementId);
    writer.flush();
    return 0;
  }
}
