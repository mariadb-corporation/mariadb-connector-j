/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.message.server;

import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
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
   * @param buf
   * @param context
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
      // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
      this.message = buf.readStringEof();
      this.sqlState = "HY000";
    }
    if (logger.isWarnEnabled()) {
      logger.warn("Error: {}", toString());
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
