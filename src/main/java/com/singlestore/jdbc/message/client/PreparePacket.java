// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.BasePreparedStatement;
import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.message.server.CachedPrepareResultPacket;
import com.singlestore.jdbc.message.server.ErrorPacket;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

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

  /**
   * COM_STMT_PREPARE packet
   *
   * <p>int[1] 0x16 COM_STMT_PREPARE header string[EOF] SQL Statement
   */
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
