// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.constants.StateChange;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;

public class OkPacket implements Completion {

  private final Logger logger;

  private final long affectedRows;
  private final long lastInsertId;

  /**
   * Parser
   *
   * @param buf packet buffer
   * @param context connection context
   */
  public OkPacket(ReadableByteBuf buf, Context context) {
    logger = Loggers.getLogger(OkPacket.class);
    buf.skip(); // ok header
    this.affectedRows = buf.readLongLengthEncodedNotNull();
    this.lastInsertId = buf.readLongLengthEncodedNotNull();
    context.setServerStatus(buf.readUnsignedShort());
    context.setWarning(buf.readUnsignedShort());

    if (buf.readableBytes() > 0 && context.hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)) {
      buf.skip(buf.readIntLengthEncodedNotNull()); // skip info
      while (buf.readableBytes() > 0) {
        ReadableByteBuf sessionStateBuf = buf.readLengthBuffer();
        while (sessionStateBuf.readableBytes() > 0) {
          switch (sessionStateBuf.readByte()) {
            case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
              ReadableByteBuf tmpBufsv;
              do {
                tmpBufsv = sessionStateBuf.readLengthBuffer();
                String variableSv = tmpBufsv.readString(tmpBufsv.readIntLengthEncodedNotNull());
                Integer lenSv = tmpBufsv.readLength();
                String valueSv = lenSv == null ? null : tmpBufsv.readString(lenSv);
                logger.debug("System variable change:  {} = {}", variableSv, valueSv);
                switch (variableSv) {
                  case "character_set_client":
                    context.setCharset(valueSv);
                    break;
                  case "connection_id":
                    context.setThreadId(Long.parseLong(valueSv));
                    break;
                  case "threads_Connected":
                    context.setTreadsConnected(Long.parseLong(valueSv));
                    break;
                }
              } while (tmpBufsv.readableBytes() > 0);
              break;

            case StateChange.SESSION_TRACK_SCHEMA:
              sessionStateBuf.readIntLengthEncodedNotNull();
              Integer dbLen = sessionStateBuf.readLength();
              String database = dbLen == null ? null : sessionStateBuf.readString(dbLen);
              context.setDatabase(database.isEmpty() ? null : database);
              logger.debug("Database change: is '{}'", database);
              break;

            default:
              // buf.skip(buf.readIntLengthEncodedNotNull());
          }
        }
      }
    }
  }

  /**
   * get affected rows
   *
   * @return affected rows
   */
  public long getAffectedRows() {
    return affectedRows;
  }

  /**
   * Get last auto generated insert id
   *
   * @return last insert id
   */
  public long getLastInsertId() {
    return lastInsertId;
  }
}
