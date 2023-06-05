// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.StateChange;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/** Ok packet parser see https://mariadb.com/kb/en/ok_packet/ */
public class OkPacket implements Completion {
  private static final Logger logger = Loggers.getLogger(OkPacket.class);

  private final long affectedRows;
  private final long lastInsertId;

  /**
   * Parser
   *
   * @param buf packet buffer
   * @param context connection context
   */
  public OkPacket(ReadableByteBuf buf, Context context) {
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
              ReadableByteBuf tmpBuf2 = sessionStateBuf.readLengthBuffer();
              String variable = tmpBuf2.readString(tmpBuf2.readIntLengthEncodedNotNull());
              Integer len = tmpBuf2.readLength();
              String value = len == null ? null : tmpBuf2.readString(len);
              logger.debug("System variable change:  {} = {}", variable, value);
              break;

            case StateChange.SESSION_TRACK_SCHEMA:
              sessionStateBuf.readIntLengthEncodedNotNull();
              Integer dbLen = sessionStateBuf.readLength();
              String database = dbLen == null ? null : sessionStateBuf.readString(dbLen);
              context.setDatabase(database.isEmpty() ? null : database);
              logger.debug("Database change: is '{}'", database);
              break;

            default:
              buf.skip(buf.readIntLengthEncodedNotNull());
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
