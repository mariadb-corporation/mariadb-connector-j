// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.context;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.PrepareCache;
import org.mariadb.jdbc.client.impl.TransactionSaver;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.message.client.RedoableClientMessage;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

/** Redo addition to Context */
public class RedoContext extends BaseContext {

  private final TransactionSaver transactionSaver;

  /**
   * Constructor
   *
   * @param hostAddress host address
   * @param handshake server handshake
   * @param clientCapabilities client capabilities
   * @param conf configuration
   * @param exceptionFactory connection exception factory
   * @param prepareCache LRU prepare cache
   */
  public RedoContext(
      HostAddress hostAddress,
      InitialHandshakePacket handshake,
      long clientCapabilities,
      Configuration conf,
      ExceptionFactory exceptionFactory,
      PrepareCache prepareCache,
      Boolean loopbackAddress) {
    super(
        hostAddress,
        handshake,
        clientCapabilities,
        conf,
        exceptionFactory,
        prepareCache,
        loopbackAddress);
    this.transactionSaver = new TransactionSaver(conf.transactionReplaySize());
  }

  /**
   * Set server status
   *
   * @param serverStatus server status
   */
  public void setServerStatus(int serverStatus) {
    this.serverStatus = serverStatus;
    if ((serverStatus & ServerStatus.IN_TRANSACTION) == 0) transactionSaver.clear();
  }

  /**
   * Save client message
   *
   * @param msg client message
   */
  public void saveRedo(ClientMessage msg) {
    if (msg instanceof RedoableClientMessage) {
      RedoableClientMessage redoMsg = (RedoableClientMessage) msg;
      redoMsg.saveParameters();
      transactionSaver.add(redoMsg);
    }
  }

  /**
   * Save client messages
   *
   * @param msgs client messages
   */
  public void saveRedo(ClientMessage[] msgs) {
    for (ClientMessage msg : msgs) saveRedo(msg);
  }

  /**
   * Get transaction saver cache
   *
   * @return transaction saver cache
   */
  public TransactionSaver getTransactionSaver() {
    return transactionSaver;
  }
}
