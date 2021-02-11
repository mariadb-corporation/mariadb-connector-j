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

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executor;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.message.client.ClientMessage;
import org.mariadb.jdbc.message.server.Completion;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public interface Client extends AutoCloseable {

  List<Completion> execute(ClientMessage message) throws SQLException;

  List<Completion> execute(ClientMessage message, org.mariadb.jdbc.Statement stmt)
      throws SQLException;

  List<Completion> execute(
      ClientMessage message,
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException;

  List<Completion> executePipeline(
      ClientMessage[] messages,
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException;

  void readStreamingResults(
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException;

  void closePrepare(PrepareResultPacket prepare) throws SQLException;

  void transactionReplay(TransactionSaver transactionSaver) throws SQLException;

  void abort(Executor executor) throws SQLException;

  void close() throws SQLException;

  void setReadOnly(boolean readOnly) throws SQLException;

  int getSocketTimeout();

  void setSocketTimeout(int milliseconds) throws SQLException;

  boolean isClosed();

  boolean isPrimary();

  Context getContext();

  ExceptionFactory getExceptionFactory();

  HostAddress getHostAddress();
}
