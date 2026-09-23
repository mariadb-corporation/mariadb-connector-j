// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.message.server;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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

  static final byte[] CHARACTER_SET_CLIENT =
      "character_set_client".getBytes(StandardCharsets.UTF_8);
  static final byte[] CONNECTION_ID = "connection_id".getBytes(StandardCharsets.UTF_8);
  static final byte[] THREAD_CONNECTED = "threads_Connected".getBytes(StandardCharsets.UTF_8);
  static final byte[] AUTO_INCREMENT_INCREMENT =
      "auto_increment_increment".getBytes(StandardCharsets.UTF_8);
  static final byte[] MAXSCALE = "maxscale".getBytes(StandardCharsets.UTF_8);
  static final byte[] REDIRECT_URL = "redirect_url".getBytes(StandardCharsets.UTF_8);
  static final byte[] TX_ISOLATION = "tx_isolation".getBytes(StandardCharsets.UTF_8);
  static final byte[] TRANSACTION_ISOLATION =
      "transaction_isolation".getBytes(StandardCharsets.UTF_8);

  static final byte[] REPEATABLE_READ = "REPEATABLE-READ".getBytes(StandardCharsets.UTF_8);
  static final byte[] READ_UNCOMMITTED = "READ-UNCOMMITTED".getBytes(StandardCharsets.UTF_8);
  static final byte[] READ_COMMITTED = "READ-COMMITTED".getBytes(StandardCharsets.UTF_8);
  static final byte[] SERIALIZABLE = "SERIALIZABLE".getBytes(StandardCharsets.UTF_8);

  private static boolean is(ReadableByteBuf buf, int pos, int len, byte[] expected) {
    return len == expected.length && Arrays.equals(buf.buf(), pos, pos + len, expected, 0, len);
  }

  /**
   * Apply one SESSION_TRACK_SYSTEM_VARIABLES entry. The variable name is compared with the tracked
   * names and numeric values are parsed directly in the packet buffer, without intermediate byte
   * arrays or strings.
   *
   * @param buf entry buffer, positioned on the length-encoded variable name
   * @param context connection context
   */
  private static void applySystemVariable(ReadableByteBuf buf, Context context)
      throws SQLException {
    // lengths are server-declared: validate them before using them
    int nameLen = buf.readIntLengthEncodedNotNull();
    buf.checkLength(nameLen);
    int namePos = buf.pos();
    buf.skip(nameLen);
    Integer valueLen = buf.readLength();
    if (valueLen == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("System variable change:  {} = null", new String(buf.buf(), namePos, nameLen));
      }
      return;
    }
    int len = valueLen;
    buf.checkLength(len);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "System variable change:  {} = {}",
          new String(buf.buf(), namePos, nameLen),
          new String(buf.buf(), buf.pos(), len));
    }

    if (is(buf, namePos, nameLen, CHARACTER_SET_CLIENT)) {
      context.setCharset(buf.readString(len));
    } else if (is(buf, namePos, nameLen, CONNECTION_ID)) {
      context.setThreadId(buf.atoll(len));
    } else if (is(buf, namePos, nameLen, THREAD_CONNECTED)) {
      context.setTreadsConnected(buf.atoll(len));
    } else if (is(buf, namePos, nameLen, AUTO_INCREMENT_INCREMENT)) {
      context.setAutoIncrement(buf.atoll(len));
    } else if (is(buf, namePos, nameLen, MAXSCALE)) {
      context.setMaxscaleVersion(buf.readString(len));
    } else if (is(buf, namePos, nameLen, REDIRECT_URL)) {
      if (len > 0) {
        context.setRedirectUrl(buf.readString(len));
      }
    } else if (is(buf, namePos, nameLen, TX_ISOLATION)
        || is(buf, namePos, nameLen, TRANSACTION_ISOLATION)) {
      int valuePos = buf.pos();
      buf.skip(len);
      if (is(buf, valuePos, len, REPEATABLE_READ)) {
        context.setTransactionIsolationLevel(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
      } else if (is(buf, valuePos, len, READ_UNCOMMITTED)) {
        context.setTransactionIsolationLevel(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
      } else if (is(buf, valuePos, len, READ_COMMITTED)) {
        context.setTransactionIsolationLevel(java.sql.Connection.TRANSACTION_READ_COMMITTED);
      } else if (is(buf, valuePos, len, SERIALIZABLE)) {
        context.setTransactionIsolationLevel(java.sql.Connection.TRANSACTION_SERIALIZABLE);
      } else {
        context.setTransactionIsolationLevel(null);
      }
    } else {
      buf.skip(len);
    }
  }

  /**
   * Parser
   *
   * @param buf packet buffer
   * @param context connection context
   */
  public static OkPacket parse(ReadableByteBuf buf, Context context) throws SQLException {
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
                  applySystemVariable(tmpBufsv, context);
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
                sessionStateBuf.skip(sessionStateBuf.readIntLengthEncodedNotNull());
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
  public static OkPacket parseWithInfo(ReadableByteBuf buf, Context context) throws SQLException {
    buf.skip(); // ok header
    long affectedRows = buf.readLongLengthEncodedNotNull();
    long lastInsertId = buf.readLongLengthEncodedNotNull();
    context.setServerStatus(buf.readUnsignedShort());
    context.setWarning(buf.readUnsignedShort());
    byte[] info;
    if (buf.readableBytes() > 0) {
      info = buf.readBytes(buf.readIntLengthEncodedNotNull());
      if (context.hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)) {
        while (buf.readableBytes() > 0) {
          ReadableByteBuf sessionStateBuf = buf.readLengthBuffer();
          while (sessionStateBuf.readableBytes() > 0) {
            switch (sessionStateBuf.readByte()) {
              case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
                ReadableByteBuf tmpBufsv;
                do {
                  tmpBufsv = sessionStateBuf.readLengthBuffer();
                  applySystemVariable(tmpBufsv, context);
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
                sessionStateBuf.skip(sessionStateBuf.readIntLengthEncodedNotNull());
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
