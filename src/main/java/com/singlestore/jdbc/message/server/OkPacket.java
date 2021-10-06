// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.constants.StateChange;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;

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
        if (buf.readLengthNotNull() > 0) {
          switch (buf.readByte()) {
            case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
              buf.readLengthNotNull();
              String variable = buf.readString(buf.readLengthNotNull());
              Integer len = buf.readLength();
              String value = len == null ? null : buf.readString(len);
              logger.debug("System variable change:  {} = {}", variable, value);
              break;

            case StateChange.SESSION_TRACK_SCHEMA:
              buf.readLengthNotNull();
              Integer dbLen = buf.readLength();
              String database = dbLen == null ? null : buf.readString(dbLen);
              context.setDatabase(database.isEmpty() ? null : database);
              logger.debug("Database change: is '{}'", database);
              break;

            default:
              buf.skip(buf.readLengthNotNull());
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
