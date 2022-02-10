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
