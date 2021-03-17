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
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.StateChange;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public class OkPacket implements Completion {
  private static final Logger logger = Loggers.getLogger(OkPacket.class);

  private final long affectedRows;
  private final long lastInsertId;

  public OkPacket(ReadableByteBuf buf, Context context) {
    buf.skip(); // ok header
    this.affectedRows = buf.readLengthNotNull();
    this.lastInsertId = buf.readLengthNotNull();
    context.setServerStatus(buf.readUnsignedShort());
    context.setWarning(buf.readUnsignedShort());

    if ((context.getServerCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0
        && buf.readableBytes() > 0) {
      buf.skip(buf.readLengthNotNull()); // skip info
      while (buf.readableBytes() > 0) {
        ReadableByteBuf stateInfo = buf.readLengthBuffer();
        if (stateInfo.readableBytes() > 0) {
          switch (stateInfo.readByte()) {
            case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
              ReadableByteBuf sessionVariableBuf = stateInfo.readLengthBuffer();
              String variable =
                  sessionVariableBuf.readString(sessionVariableBuf.readLengthNotNull());
              Integer len = sessionVariableBuf.readLength();
              String value = len == null ? null : sessionVariableBuf.readString(len);
              if (logger.isDebugEnabled()) {
                logger.debug("System variable change :  {} = {}", variable, value);
              }
              break;

            case StateChange.SESSION_TRACK_SCHEMA:
              ReadableByteBuf sessionSchemaBuf = stateInfo.readLengthBuffer();
              Integer dbLen = sessionSchemaBuf.readLength();
              String database = dbLen == null ? null : sessionSchemaBuf.readString(dbLen);
              context.setDatabase(database);
              if (logger.isDebugEnabled()) {
                logger.debug("Database change : now is '{}'", database);
              }
              break;

            default:
              // eat;
          }
        }
      }
    }
  }

  public long getAffectedRows() {
    return affectedRows;
  }

  public long getLastInsertId() {
    return lastInsertId;
  }
}
