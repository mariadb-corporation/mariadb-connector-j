/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
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

  public String getSql() {
    return sql;
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

    ReadableByteBuf buf = reader.readReadablePacket(true, traceEnable);
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
          return previousCached != null ? previousCached : prepare;
        }
        return new PrepareResultPacket(buf, reader, context);
    }
  }
}
