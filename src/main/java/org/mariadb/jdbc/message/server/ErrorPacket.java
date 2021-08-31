// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.ServerMessage;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public final class ErrorPacket implements ServerMessage {
  private static final Logger logger = Loggers.getLogger(ErrorPacket.class);
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
    byte next = buf.getByte(buf.pos());
    if (next == (byte) '#') {
      buf.skip(); // skip '#'
      this.sqlState = buf.readAscii(5);
      this.message = buf.readStringEof();
    } else {
      // Pre-4.1 message, still can be output in newer versions (e.g. with 'Too many connections')
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
