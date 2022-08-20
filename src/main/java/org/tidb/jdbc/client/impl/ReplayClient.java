// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.client.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.tidb.jdbc.Configuration;
import org.tidb.jdbc.HostAddress;
import org.tidb.jdbc.Statement;
import org.tidb.jdbc.client.Completion;
import org.tidb.jdbc.client.context.RedoContext;
import org.tidb.jdbc.export.MaxAllowedPacketException;
import org.tidb.jdbc.export.Prepare;
import org.tidb.jdbc.message.ClientMessage;
import org.tidb.jdbc.message.client.*;
import org.tidb.jdbc.message.client.PreparePacket;
import org.tidb.jdbc.message.client.RedoableClientMessage;
import org.tidb.jdbc.message.client.RedoableWithPrepareClientMessage;
import org.tidb.jdbc.message.server.PrepareResultPacket;

/** Replay client wrapper */
public class ReplayClient extends StandardClient {

  /**
   * Constructor
   *
   * @param conf configuration
   * @param hostAddress host
   * @param lock thread lock object
   * @param skipPostCommands must skip connection post commands
   * @throws SQLException if connection fails
   */
  public ReplayClient(
      Configuration conf, HostAddress hostAddress, ReentrantLock lock, boolean skipPostCommands)
      throws SQLException {
    super(conf, hostAddress, lock, skipPostCommands);
  }

  @Override
  public int sendQuery(ClientMessage message) throws SQLException {
    checkNotClosed();
    try {
      if (message instanceof RedoableClientMessage)
        ((RedoableClientMessage) message).ensureReplayable(context);
      return message.encode(writer, context);
    } catch (IOException ioException) {
      if (ioException instanceof MaxAllowedPacketException) {
        if (((MaxAllowedPacketException) ioException).isMustReconnect()) {
          destroySocket();
          throw exceptionFactory
              .withSql(message.description())
              .create(
                  "Packet too big for current server max_allowed_packet value",
                  "08000",
                  ioException);
        }
        throw exceptionFactory
            .withSql(message.description())
            .create(
                "Packet too big for current server max_allowed_packet value", "HZ000", ioException);
      }
      destroySocket();
      throw exceptionFactory
          .withSql(message.description())
          .create("Socket error", "08000", ioException);
    }
  }

  @Override
  public List<Completion> executePipeline(
      ClientMessage[] messages,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    List<Completion> res =
        super.executePipeline(
            messages,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion,
            canRedo);
    ((RedoContext) context).saveRedo(messages);
    return res;
  }

  @Override
  public List<Completion> execute(
      ClientMessage message,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    List<Completion> completions =
        super.execute(
            message,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion,
            canRedo);
    ((RedoContext) context).saveRedo(message);
    return completions;
  }

  /**
   * Replay transaction, re-prepare server command if needed
   *
   * @param transactionSaver transaction cache
   * @throws SQLException if any error occurs
   */
  public void transactionReplay(TransactionSaver transactionSaver) throws SQLException {
    RedoableClientMessage[] buffers = transactionSaver.getBuffers();
    try {
      // replay all but last
      Prepare prepare;
      for (int i = 0; i < transactionSaver.getIdx(); i++) {
        RedoableClientMessage querySaver = buffers[i];
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
