// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.message.ServerMessage;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;

public final class ErrorPacket implements ServerMessage {
  private final short errorCode;
  private final String message;
  private final String sqlState;

  /**
   * +--------------------------------------------------+ | 0 1 2 3 4 5 6 7 8 9 a b c d e f |
   * +--------------------------------------------------+------------------+ | 17 00 00 01 FF 10 04
   * 54 6F 6F 20 6D 61 6E 79 20 | .......Too many | | 63 6F 6E 6E 65 63 74 69 6F 6E 73 | connections
   * | +--------------------------------------------------+------------------+
   *
   * @param buf error packet buffer
   * @param context current context
   */
  public ErrorPacket(ReadableByteBuf buf, Context context) {
    buf.skip();
    this.errorCode = buf.readShort();
    Logger logger = Loggers.getLogger(ErrorPacket.class);
    byte next = buf.getByte(buf.pos());
    if (next == (byte) '#') {
      buf.skip(); // skip '#'
      this.sqlState = buf.readAscii(5);
      this.message = buf.readStringEof();
    } else {
      // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
      this.message = buf.readStringEof();
      this.sqlState = "HY000";
    }
    if (logger.isWarnEnabled()) {
      logger.warn("Error: {}-{}: {}", errorCode, sqlState, message);
    }

    // force current status to in transaction to ensure rollback/commit, since command may have
    // issue a transaction
    if (context != null) {
      int serverStatus = context.getServerStatus();
      serverStatus |= ServerStatus.IN_TRANSACTION;
      context.setServerStatus(serverStatus);
    }
  }

  public short getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }

  public String getSqlState() {
    return sqlState;
  }
}
