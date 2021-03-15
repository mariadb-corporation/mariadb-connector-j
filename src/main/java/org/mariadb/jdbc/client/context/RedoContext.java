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

package org.mariadb.jdbc.client.context;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.PrepareCache;
import org.mariadb.jdbc.client.TransactionSaver;
import org.mariadb.jdbc.message.client.ClientMessage;
import org.mariadb.jdbc.message.client.RedoableClientMessage;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public class RedoContext extends BaseContext {

  private final TransactionSaver transactionSaver;

  public RedoContext(
      InitialHandshakePacket handshake,
      Configuration conf,
      ExceptionFactory exceptionFactory,
      PrepareCache prepareCache) {
    super(handshake, conf, exceptionFactory, prepareCache);
    this.transactionSaver = new TransactionSaver();
  }

  public void setServerStatus(int serverStatus) {
    this.serverStatus = serverStatus;
    if ((serverStatus & ServerStatus.IN_TRANSACTION) == 0) transactionSaver.clear();
  }

  public void saveRedo(ClientMessage msg) {
    if (msg instanceof RedoableClientMessage) {
      RedoableClientMessage redoMsg = (RedoableClientMessage) msg;
      redoMsg.saveParameters();
      transactionSaver.add(redoMsg);
    }
  }

  public void saveRedo(ClientMessage[] msgs) {
    for (ClientMessage msg : msgs) saveRedo(msg);
  }

  public TransactionSaver getTransactionSaver() {
    return transactionSaver;
  }
}
