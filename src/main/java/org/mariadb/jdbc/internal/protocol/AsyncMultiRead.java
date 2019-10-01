/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.com.read.dao.*;
import org.mariadb.jdbc.internal.com.send.*;
import org.mariadb.jdbc.internal.com.send.parameters.*;
import org.mariadb.jdbc.internal.util.*;
import org.mariadb.jdbc.internal.util.dao.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.mariadb.jdbc.internal.util.SqlStates.*;

public class AsyncMultiRead implements Callable<AsyncMultiReadResult> {

  private final ComStmtPrepare comStmtPrepare;
  private final BulkStatus status;
  private final int sendCmdInitialCounter;
  private final Protocol protocol;
  private final boolean readPrepareStmtResult;
  private final AbstractMultiSend bulkSend;
  private final List<ParameterHolder[]> parametersList;
  private final List<String> queries;
  private final Results results;
  private final int paramCount;
  private final AsyncMultiReadResult asyncMultiReadResult;

  /**
   * Read results async to avoid local and remote networking stack buffer overflow "lock".
   *
   * @param comStmtPrepare current prepare
   * @param status bulk status
   * @param protocol protocol
   * @param readPrepareStmtResult must read prepare statement result
   * @param bulkSend bulk sender object
   * @param paramCount number of parameters
   * @param results execution result
   * @param parametersList parameter list
   * @param queries queries
   * @param prepareResult prepare result
   */
  public AsyncMultiRead(
      ComStmtPrepare comStmtPrepare,
      BulkStatus status,
      Protocol protocol,
      boolean readPrepareStmtResult,
      AbstractMultiSend bulkSend,
      int paramCount,
      Results results,
      List<ParameterHolder[]> parametersList,
      List<String> queries,
      PrepareResult prepareResult) {
    this.comStmtPrepare = comStmtPrepare;
    this.status = status;
    this.sendCmdInitialCounter = status.sendCmdCounter - 1;
    this.protocol = protocol;
    this.readPrepareStmtResult = readPrepareStmtResult;
    this.bulkSend = bulkSend;
    this.paramCount = paramCount;
    this.results = results;
    this.parametersList = parametersList;
    this.queries = queries;
    this.asyncMultiReadResult = new AsyncMultiReadResult(prepareResult);
  }

  @Override
  public AsyncMultiReadResult call() throws Exception {
    // avoid synchronisation of calls for write and read
    // since technically, getResult can be called before the write is send.
    // Other solution would have been to synchronised write and read, but would have been less
    // performant,
    // just to have this timeout according to set value
    int initialTimeout = protocol.getTimeout();
    if (initialTimeout != 0) {
      protocol.changeSocketSoTimeout(0);
    }

    if (readPrepareStmtResult) {
      try {
        asyncMultiReadResult.setPrepareResult(
            comStmtPrepare.read(protocol.getReader(), protocol.isEofDeprecated()));
      } catch (SQLException queryException) {
        asyncMultiReadResult.setException(queryException);
      }
    }

    // read all corresponding results
    int counter = 0;

    // ensure to not finished loop while all bulk has not been send
    outerloop:
    while (!status.sendEnded || counter < status.sendSubCmdCounter) {
      // read results for each send data
      while (counter < status.sendSubCmdCounter) {
        try {
          protocol.getResult(results);
        } catch (SQLException qex) {
          if (qex instanceof SQLNonTransientConnectionException
              || qex instanceof SQLTransientConnectionException) {
            asyncMultiReadResult.setException(qex);
            break outerloop;
          }
          if (asyncMultiReadResult.getException() == null) {
            asyncMultiReadResult.setException(
                bulkSend.handleResultException(
                    qex,
                    results,
                    parametersList,
                    queries,
                    counter,
                    sendCmdInitialCounter,
                    paramCount,
                    asyncMultiReadResult.getPrepareResult()));
          }
        }
        counter++;

        if (Thread.currentThread().isInterrupted()) {
          asyncMultiReadResult.setException(
              new SQLException(
                  "Interrupted reading responses ", INTERRUPTED_EXCEPTION.getSqlState(), -1));
          break;
        }
      }
    }

    if (initialTimeout != 0) {
      protocol.changeSocketSoTimeout(initialTimeout);
    }

    return asyncMultiReadResult;
  }
}
