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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.client.result.StreamingResult;
import org.mariadb.jdbc.client.result.UpdatableResult;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.message.server.Completion;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.OkPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public interface ClientMessage {

  int encode(PacketWriter writer, Context context) throws IOException, SQLException;

  default int batchUpdateLength() {
    return 0;
  }

  String description();

  default boolean binaryProtocol() {
    return false;
  }

  default Completion readPacket(
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      PacketReader reader,
      Context context,
      ExceptionFactory exceptionFactory,
      ReentrantLock lock,
      boolean traceEnable)
      throws IOException, SQLException {

    ReadableByteBuf buf = reader.readReadablePacket(true, traceEnable);

    switch (buf.getUnsignedByte()) {

        // *********************************************************************************************************
        // * OK response
        // *********************************************************************************************************
      case 0x00:
        return new OkPacket(buf, context);

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

        // *********************************************************************************************************
        // * ResultSet
        // *********************************************************************************************************
      default:
        int fieldCount = buf.readLengthNotNull();

        // read columns information's
        ColumnDefinitionPacket[] ci = new ColumnDefinitionPacket[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          ci[i] = new ColumnDefinitionPacket(reader.readReadablePacket(false, traceEnable));
        }

        if (!context.isEofDeprecated()) {
          // skip intermediate EOF
          reader.readReadablePacket(true, traceEnable);
        }

        // read resultSet
        if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
          return new UpdatableResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        }

        if (fetchSize != 0) {
          if ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
            context.setServerStatus(context.getServerStatus() - ServerStatus.MORE_RESULTS_EXISTS);
          }

          return new StreamingResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              fetchSize,
              lock,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        } else {
          return new CompleteResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        }
    }
  }
}
