// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.BasePreparedStatement;
import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.*;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketReader;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.message.server.CachedPrepareResultPacket;
import com.singlestore.jdbc.message.server.Completion;
import com.singlestore.jdbc.message.server.ErrorPacket;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import com.singlestore.jdbc.util.exceptions.ExceptionFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

public final class PreparePacket implements ClientMessage {
  private final String sql;

  public PreparePacket(String sql) {
    this.sql = sql;
  }

  @Override
  public int encode(PacketWriter writer, Context context) throws IOException {
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
      PacketReader reader,
      PacketWriter writer,
      Context context,
      ExceptionFactory exceptionFactory,
      ReentrantLock lock,
      boolean traceEnable)
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
      CachedPrepareResultPacket prepare = new CachedPrepareResultPacket(buf, reader, context);
      PrepareResultPacket previousCached =
          context
              .getPrepareCache()
              .put(
                  sql,
                  prepare,
                  stmt instanceof ServerPreparedStatement ? (ServerPreparedStatement) stmt : null);
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
    return sql;
  }
}
