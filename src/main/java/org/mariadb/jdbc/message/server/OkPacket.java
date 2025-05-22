// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.StateChange;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/** Ok packet parser see https://mariadb.com/kb/en/ok_packet/ */
public class OkPacket implements Completion {
  private static final OkPacket BASIC_OK = new OkPacket(0, 0, null);
  private static final Logger logger = Loggers.getLogger(OkPacket.class);

  private final long affectedRows;
  private final long lastInsertId;
  private final byte[] info;

  private OkPacket(long affectedRows, long lastInsertId, byte[] info) {
    this.affectedRows = affectedRows;
    this.lastInsertId = lastInsertId;
    this.info = info;
  }

  static byte[] CHARACTER_SET_CLIENT = "character_set_client".getBytes(StandardCharsets.UTF_8);
  static byte[] CONNECTION_ID = "connection_id".getBytes(StandardCharsets.UTF_8);
  static byte[] THREAD_CONNECTED = "threads_Connected".getBytes(StandardCharsets.UTF_8);
  static byte[] AUTO_INCREMENT_INCREMENT =
      "auto_increment_increment".getBytes(StandardCharsets.UTF_8);
  static byte[] REDIRECT_URL = "redirect_url".getBytes(StandardCharsets.UTF_8);
  static byte[] TX_ISOLATION = "tx_isolation".getBytes(StandardCharsets.UTF_8);
  static byte[] TRANSACTION_ISOLATION = "transaction_isolation".getBytes(StandardCharsets.UTF_8);

  static byte[] REPEATABLE_READ = "REPEATABLE-READ".getBytes(StandardCharsets.UTF_8);
  static byte[] READ_UNCOMMITTED = "READ-UNCOMMITTED".getBytes(StandardCharsets.UTF_8);
  static byte[] READ_COMMITTED = "READ-COMMITTED".getBytes(StandardCharsets.UTF_8);
  static byte[] SERIALIZABLE = "SERIALIZABLE".getBytes(StandardCharsets.UTF_8);

  /**
   * Parser
   *
   * @param buf packet buffer
   * @param context connection context
   */
  public static OkPacket parse(ReadableByteBuf buf, Context context) throws IOException {
    buf.skip(); // ok header
    long affectedRows = buf.readLongLengthEncodedNotNull();
    long lastInsertId = buf.readLongLengthEncodedNotNull();
    context.setServerStatus(buf.readUnsignedShort());
    context.setWarning(buf.readUnsignedShort());

    if (buf.readableBytes() > 0) {
      buf.skip(buf.readIntLengthEncodedNotNull()); // skip info
      if (context.hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)) {
        while (buf.readableBytes() > 0) {
          ReadableByteBuf sessionStateBuf = buf.readLengthBuffer();
          while (sessionStateBuf.readableBytes() > 0) {
            switch (sessionStateBuf.readByte()) {
              case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
                ReadableByteBuf tmpBufsv;
                do {
                  tmpBufsv = sessionStateBuf.readLengthBuffer();
                  int len = tmpBufsv.readIntLengthEncodedNotNull();
                  byte[] variableBytes = new byte[len];
                  tmpBufsv.readBytes(variableBytes);

                  Integer lenSv = tmpBufsv.readLength();
                  byte[] valueBytes;
                  if (lenSv == null) {
                    valueBytes = null;
                  } else {
                    valueBytes = new byte[lenSv];
                    tmpBufsv.readBytes(valueBytes);
                  }

                  if (logger.isDebugEnabled())
                    logger.debug(
                        "System variable change:  {} = {}",
                        new String(variableBytes, 0, len),
                        valueBytes == null ? "null" : new String(valueBytes, 0, lenSv));

                  if (Arrays.equals(CHARACTER_SET_CLIENT, variableBytes)) {
                    context.setCharset(new String(valueBytes, 0, lenSv));
                  } else if (Arrays.equals(CONNECTION_ID, variableBytes)) {
                    context.setThreadId(Long.parseLong(new String(valueBytes, 0, lenSv)));
                  } else if (Arrays.equals(THREAD_CONNECTED, variableBytes)) {
                    context.setTreadsConnected(Long.parseLong(new String(valueBytes, 0, lenSv)));
                  } else if (Arrays.equals(AUTO_INCREMENT_INCREMENT, variableBytes)) {
                    context.setAutoIncrement(Long.parseLong(new String(valueBytes, 0, lenSv)));
                  } else if (Arrays.equals(REDIRECT_URL, variableBytes)) {
                    if (lenSv != null && lenSv > 0)
                      context.setRedirectUrl(new String(valueBytes, 0, lenSv));
                  } else if (Arrays.equals(TX_ISOLATION, variableBytes)
                      || Arrays.equals(TRANSACTION_ISOLATION, variableBytes)) {
                    if (Arrays.equals(REPEATABLE_READ, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_REPEATABLE_READ);
                    } else if (Arrays.equals(READ_UNCOMMITTED, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
                    } else if (Arrays.equals(READ_COMMITTED, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_READ_COMMITTED);
                    } else if (Arrays.equals(SERIALIZABLE, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_SERIALIZABLE);
                    } else context.setTransactionIsolationLevel(null);
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
    }
    if (affectedRows == 0 && lastInsertId == 0) return BASIC_OK;
    return new OkPacket(affectedRows, lastInsertId, null);
  }

  /**
   * Parser
   *
   * @param buf packet buffer
   * @param context connection context
   * @return Ok_Packet object
   */
  public static OkPacket parseWithInfo(ReadableByteBuf buf, Context context) throws IOException {
    buf.skip(); // ok header
    long affectedRows = buf.readLongLengthEncodedNotNull();
    long lastInsertId = buf.readLongLengthEncodedNotNull();
    context.setServerStatus(buf.readUnsignedShort());
    context.setWarning(buf.readUnsignedShort());
    byte[] info;
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
                  int len = tmpBufsv.readIntLengthEncodedNotNull();
                  byte[] variableBytes = new byte[len];
                  tmpBufsv.readBytes(variableBytes);

                  Integer lenSv = tmpBufsv.readLength();
                  byte[] valueBytes;
                  if (lenSv == null) {
                    valueBytes = null;
                  } else {
                    valueBytes = new byte[lenSv];
                    tmpBufsv.readBytes(valueBytes);
                  }

                  if (logger.isDebugEnabled())
                    logger.debug(
                        "System variable change:  {} = {}",
                        new String(variableBytes, 0, len),
                        valueBytes == null ? "null" : new String(valueBytes, 0, lenSv));

                  if (Arrays.equals(CHARACTER_SET_CLIENT, variableBytes)) {
                    context.setCharset(new String(valueBytes, 0, lenSv));
                  } else if (Arrays.equals(CONNECTION_ID, variableBytes)) {
                    context.setThreadId(Long.parseLong(new String(valueBytes, 0, lenSv)));
                  } else if (Arrays.equals(THREAD_CONNECTED, variableBytes)) {
                    context.setTreadsConnected(Long.parseLong(new String(valueBytes, 0, lenSv)));
                  } else if (Arrays.equals(AUTO_INCREMENT_INCREMENT, variableBytes)) {
                    context.setAutoIncrement(Long.parseLong(new String(valueBytes, 0, lenSv)));
                  } else if (Arrays.equals(REDIRECT_URL, variableBytes)) {
                    if (lenSv != null && lenSv > 0)
                      context.setRedirectUrl(new String(valueBytes, 0, lenSv));
                  } else if (Arrays.equals(TX_ISOLATION, variableBytes)
                      || Arrays.equals(TRANSACTION_ISOLATION, variableBytes)) {
                    if (Arrays.equals(REPEATABLE_READ, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_REPEATABLE_READ);
                    } else if (Arrays.equals(READ_UNCOMMITTED, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
                    } else if (Arrays.equals(READ_COMMITTED, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_READ_COMMITTED);
                    } else if (Arrays.equals(SERIALIZABLE, valueBytes)) {
                      context.setTransactionIsolationLevel(
                          java.sql.Connection.TRANSACTION_SERIALIZABLE);
                    } else context.setTransactionIsolationLevel(null);
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
    } else info = new byte[0];
    return new OkPacket(affectedRows, lastInsertId, info);
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

  /**
   * Get ok packet info byte array. That is usually a string value, but for first Ok_Packet,
   * containing fingerprint info.
   *
   * @return info
   */
  public byte[] getInfo() {
    return info;
  }
}
