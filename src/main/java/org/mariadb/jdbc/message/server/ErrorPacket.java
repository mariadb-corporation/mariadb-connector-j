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

  public ErrorPacket(ReadableByteBuf buf, Context context) {
    buf.skip();
    this.errorCode = buf.readShort();
    byte next = buf.getByte(buf.pos());
    if (next == (byte) '#') {
      buf.skip(); // skip '#'
      this.sqlState = buf.readAscii(5);
      this.message = buf.readStringEof();
    } else {
      this.message = buf.readStringEof();
      this.sqlState = "HY000";
    }
    if (logger.isWarnEnabled()) {
      logger.warn("Error: {}", toString());
    }

    // force current status to in transaction to ensure rollback/commit, since command may have
    // issue a transaction
    int serverStatus = context.getServerStatus();
    serverStatus |= ServerStatus.IN_TRANSACTION;
    context.setServerStatus(serverStatus);
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

  @Override
  public String toString() {
    return "ErrorPacket{"
        + "errorCode="
        + errorCode
        + ", message='"
        + message
        + '\''
        + ", sqlState='"
        + sqlState
        + "'}";
  }
}
