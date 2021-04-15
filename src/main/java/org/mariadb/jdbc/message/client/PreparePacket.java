// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;
import org.mariadb.jdbc.message.server.Completion;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

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
    switch (buf.getUnsignedByte()) {
        // *********************************************************************************************************
        // * ERROR response
        // *********************************************************************************************************
      case 0xff:
        // force current status to in transaction to ensure rollback/commit, since command may
        // have issue a transaction
        ErrorPacket errorPacket = new ErrorPacket(buf, context);
        throw exceptionFactory
            .withSql(this.description())
            .create(
                errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
      default:
        if (context.getConf().useServerPrepStmts()
            && context.getConf().cachePrepStmts()
            && sql.length() < 1000) {
          CachedPrepareResultPacket prepare = new CachedPrepareResultPacket(buf, reader, context);
          PrepareResultPacket previousCached =
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
  }

  @Override
  public String description() {
    return sql;
  }
}
