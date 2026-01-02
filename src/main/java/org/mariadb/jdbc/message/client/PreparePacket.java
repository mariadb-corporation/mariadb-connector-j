// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import static org.mariadb.jdbc.message.client.CommandConstants.COM_STMT_PREPARE;

import java.io.IOException;
import java.sql.SQLException;
import java.util.function.Consumer;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

/** Send a client COM_STMT_PREPARE packet see https://mariadb.com/kb/en/com_stmt_prepare/ */
public final class PreparePacket implements ClientMessage {
  private final String sql;

  /**
   * Construct prepare packet
   *
   * @param sql sql command
   */
  public PreparePacket(String sql) {
    this.sql = sql;
  }

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.initPacket();
    writer.writeByte(COM_STMT_PREPARE);
    writer.writeString(this.sql);
    writer.flush();
    return 1;
  }

  @Override
  public Completion readPacket(
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      Reader reader,
      Writer writer,
      Context context,
      ExceptionFactory exceptionFactory,
      ClosableLock lock,
      boolean traceEnable,
      ClientMessage message,
      Consumer<String> redirectFct)
      throws IOException, SQLException {

    ReadableByteBuf buf = reader.readReusablePacket(traceEnable);
    // *********************************************************************************************************
    // * ERROR response
    // *********************************************************************************************************
    if (buf.getUnsignedByte()
        == 0xff) { // force current status to in transaction to ensure rollback/commit, since
      // command may
      // have issue a transaction
      ErrorPacket errorPacket = new ErrorPacket(buf, context);
      throw exceptionFactory
          .withSql(this.description())
          .create(
              errorPacket.getMessage(),
              errorPacket.getSqlState(),
              errorPacket.getErrorCode(),
              true);
    }
    if (context.getConf().prepareThreshold() >= 0
        && context.getConf().cachePrepStmts()
        && sql.length() < 8192) {
      PrepareResultPacket prepare = new CachedPrepareResultPacket(buf, reader, context);
      PrepareResultPacket previousCached =
          (PrepareResultPacket) context.putPrepareCacheCmd(sql, prepare);
      PrepareResultPacket result = previousCached != null ? previousCached : prepare;
      // Increment use count for the prepare result being used
      if (result instanceof CachedPrepareResultPacket) {
        ((CachedPrepareResultPacket) result).incrementUse();
      }
      if (stmt != null) {
        ((BasePreparedStatement) stmt).setPrepareResult(result);
      }
      return result;
    }
    PrepareResultPacket prepareResult = new PrepareResultPacket(buf, reader, context);
    if (stmt != null) {
      ((BasePreparedStatement) stmt).setPrepareResult(prepareResult);
    }
    return prepareResult;
  }

  @Override
  public String description() {
    return "PREPARE " + sql;
  }
}
