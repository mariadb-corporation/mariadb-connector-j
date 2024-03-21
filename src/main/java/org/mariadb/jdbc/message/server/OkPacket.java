// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
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
  private final byte[] info;

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

    if (buf.readableBytes() > 0) {
      info = new byte[buf.readIntLengthEncodedNotNull()];
      buf.readBytes(info);
      if (context.hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)) {
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
                    case "auto_increment_increment":
                      context.setAutoIncrement(Long.parseLong(valueSv));
                      break;
                    case "redirect_url":
                      context.setRedirectUrl(valueSv);
                      break;
                    case "tx_isolation":
                    case "transaction_isolation":
                      switch (valueSv) {
                        case "REPEATABLE-READ":
                          context.setTransactionIsolationLevel(
                              java.sql.Connection.TRANSACTION_REPEATABLE_READ);
                          break;
                        case "READ-UNCOMMITTED":
                          context.setTransactionIsolationLevel(
                              java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
                          break;
                        case "READ-COMMITTED":
                          context.setTransactionIsolationLevel(
                              java.sql.Connection.TRANSACTION_READ_COMMITTED);
                          break;
                        case "SERIALIZABLE":
                          context.setTransactionIsolationLevel(
                              java.sql.Connection.TRANSACTION_SERIALIZABLE);
                          break;
                        default:
                          context.setTransactionIsolationLevel(null);
                          break;
                      }
                      break;
                  }
                } while (tmpBufsv.readableBytes() > 0);
                break;

              case StateChange.SESSION_TRACK_SCHEMA:
                sessionStateBuf.readIntLengthEncodedNotNull();
                Integer dbLen = sessionStateBuf.readLength();
                String database =
                    dbLen == null || dbLen == 0 ? null : sessionStateBuf.readString(dbLen);
                context.setDatabase(database);
                logger.debug("Database change: is '{}'", database);
                break;

              default:
                buf.skip(buf.readIntLengthEncodedNotNull());
                break;
            }
          }
        }
      }
    } else {
      info = null;
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

  public byte[] getInfo() {
    return info;
  }
}
