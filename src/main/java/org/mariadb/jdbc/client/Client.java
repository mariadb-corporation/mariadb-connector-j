// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executor;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.ClientMessage;

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

  void closePrepare(Prepare prepare) throws SQLException;

  void abort(Executor executor) throws SQLException;

  void close() throws SQLException;

  void setReadOnly(boolean readOnly) throws SQLException;

  int getWaitTimeout();

  int getSocketTimeout();

  void setSocketTimeout(int milliseconds) throws SQLException;

  boolean isClosed();

  void reset();

  boolean isPrimary();

  Context getContext();

  ExceptionFactory getExceptionFactory();

  HostAddress getHostAddress();
}
