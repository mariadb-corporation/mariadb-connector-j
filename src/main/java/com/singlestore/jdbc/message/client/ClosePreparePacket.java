// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

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

  /** COM_STMT_CLOSE packet. int<1> 0x19 COM_STMT_CLOSE header int<4> Statement id */
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
