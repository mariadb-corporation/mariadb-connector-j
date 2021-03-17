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

package org.mariadb.jdbc.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.context.BaseContext;
import org.mariadb.jdbc.client.context.RedoContext;
import org.mariadb.jdbc.client.socket.*;
import org.mariadb.jdbc.message.client.*;
import org.mariadb.jdbc.message.server.Completion;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public class ClientReplayImpl extends ClientImpl {
  private static final Logger logger = Loggers.getLogger(ClientReplayImpl.class);

  public ClientReplayImpl(
      Configuration conf, HostAddress hostAddress, ReentrantLock lock, boolean skipPostCommands)
      throws SQLException {
    super(conf, hostAddress, lock, skipPostCommands);
    this.context = RedoContext.from((BaseContext) this.context);
  }

  @Override
  public int sendQuery(ClientMessage message) throws SQLException {
    checkNotClosed();
    try {
      if (message instanceof RedoableClientMessage)
        ((RedoableClientMessage) message).ensureReplayable(context);
      return message.encode(writer, context);
    } catch (IOException ioException) {
      destroySocket();
      throw context
          .getExceptionFactory()
          .withSql(message.description())
          .create("Socket error", "08000", ioException);
    }
  }

  @Override
  public List<Completion> executePipeline(
      ClientMessage[] messages,
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    List<Completion> res =
        super.executePipeline(
            messages,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion);
    context.saveRedo(messages);
    return res;
  }

  @Override
  public List<Completion> execute(
      ClientMessage message,
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    List<Completion> completions =
        super.execute(
            message,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion);
    context.saveRedo(message);
    return completions;
  }

  public void transactionReplay(TransactionSaver transactionSaver) throws SQLException {
    List<RedoableClientMessage> buffers = transactionSaver.getBuffers();
    try {
      // replay all but last
      PrepareResultPacket prepare;
      for (RedoableClientMessage querySaver : buffers) {
        int responseNo;
        if (querySaver instanceof RedoableWithPrepareClientMessage) {
          // command is a prepare statement query
          // redo on new connection need to re-prepare query
          // and substitute statement id
          RedoableWithPrepareClientMessage redoable =
              ((RedoableWithPrepareClientMessage) querySaver);
          String cmd = redoable.getCommand();
          prepare = context.getPrepareCache().get(cmd, redoable.prep());
          if (prepare == null) {
            PreparePacket preparePacket = new PreparePacket(cmd);
            sendQuery(preparePacket);
            prepare = (PrepareResultPacket) readPacket(preparePacket);
          }
          responseNo = querySaver.reEncode(writer, context, prepare);
        } else {
          responseNo = querySaver.reEncode(writer, context, null);
        }
        for (int j = 0; j < responseNo; j++) {
          readResponse(querySaver);
        }
      }
    } catch (IOException e) {
      throw context
          .getExceptionFactory()
          .create("Socket error during transaction replay", "08000", e);
    }
  }
}
