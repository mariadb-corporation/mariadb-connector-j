// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import org.tidb.jdbc.BasePreparedStatement;
import org.tidb.jdbc.ServerPreparedStatement;
import org.tidb.jdbc.Statement;
import org.tidb.jdbc.client.Completion;
import org.tidb.jdbc.client.Context;
import org.tidb.jdbc.client.ReadableByteBuf;
import org.tidb.jdbc.client.socket.Reader;
import org.tidb.jdbc.client.socket.Writer;
import org.tidb.jdbc.export.ExceptionFactory;
import org.tidb.jdbc.message.ClientMessage;
import org.tidb.jdbc.message.server.CachedPrepareResultPacket;
import org.tidb.jdbc.message.server.ErrorPacket;
import org.tidb.jdbc.message.server.PrepareResultPacket;

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
    writer.writeByte(0x16);
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
      ReentrantLock lock,
      boolean traceEnable,
      ClientMessage message)
      throws IOException, SQLException {

    ReadableByteBuf buf = reader.readPacket(true, traceEnable);
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
          .create(errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
    }
    if (context.getConf().useServerPrepStmts()
        && context.getConf().cachePrepStmts()
        && sql.length() < 8192) {
      PrepareResultPacket prepare = new CachedPrepareResultPacket(buf, reader, context);
      PrepareResultPacket previousCached =
          (PrepareResultPacket)
              context
                  .getPrepareCache()
                  .put(
                      sql,
                      prepare,
                      stmt instanceof ServerPreparedStatement
                          ? (ServerPreparedStatement) stmt
                          : null);
      if (stmt != null) {
        ((BasePreparedStatement) stmt)
            .setPrepareResult(previousCached != null ? previousCached : prepare);
      }
      return previousCached != null ? previousCached : prepare;
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
