// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.context;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.PrepareCache;
import com.singlestore.jdbc.client.impl.TransactionSaver;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.message.client.RedoableClientMessage;
import com.singlestore.jdbc.message.server.InitialHandshakePacket;
import com.singlestore.jdbc.util.constants.ServerStatus;

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
      PrepareCache prepareCache) {
    super(hostAddress, handshake, clientCapabilities, conf, exceptionFactory, prepareCache);
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
