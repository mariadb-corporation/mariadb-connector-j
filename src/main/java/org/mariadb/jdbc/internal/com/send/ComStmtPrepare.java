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

package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.read.ErrorPacket;
import org.mariadb.jdbc.internal.com.read.resultset.ColumnDefinition;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.io.IOException;
import java.sql.SQLException;

import static org.mariadb.jdbc.internal.com.Packet.*;

public class ComStmtPrepare {

  private final Protocol protocol;
  private final String sql;

  public ComStmtPrepare(Protocol protocol, String sql) {
    this.protocol = protocol;
    this.sql = sql;
  }

  /**
   * Send directly to socket the sql data.
   *
   * @param pos the writer
   * @throws IOException if connection error occur
   */
  public void send(PacketOutputStream pos) throws IOException {
    pos.startPacket(0);
    pos.write(COM_STMT_PREPARE);
    pos.write(this.sql);
    pos.flush();
  }

  /**
   * Read COM_PREPARE_RESULT.
   *
   * @param reader inputStream
   * @param eofDeprecated are EOF_packet deprecated
   * @return ServerPrepareResult prepare result
   * @throws IOException if connection has error
   * @throws SQLException if server answer with error.
   */
  public ServerPrepareResult read(PacketInputStream reader, boolean eofDeprecated)
      throws IOException, SQLException {
    Buffer buffer = reader.getPacket(true);
    byte firstByte = buffer.getByteAt(buffer.position);

    if (firstByte == ERROR) {
      throw buildErrorException(buffer);
    }

    if (firstByte == OK) {

      /* Prepared Statement OK */
      buffer.readByte(); /* skip field count */
      final int statementId = buffer.readInt();
      final int numColumns = buffer.readShort() & 0xffff;
      final int numParams = buffer.readShort() & 0xffff;

      ColumnDefinition[] params = new ColumnDefinition[numParams];
      ColumnDefinition[] columns = new ColumnDefinition[numColumns];

      if (numParams > 0) {
        for (int i = 0; i < numParams; i++) {
          params[i] = new ColumnDefinition(reader.getPacket(false));
        }

        if (numColumns > 0) {
          if (!eofDeprecated) {
            protocol.skipEofPacket();
          }
          for (int i = 0; i < numColumns; i++) {
            columns[i] = new ColumnDefinition(reader.getPacket(false));
          }
        }
        if (!eofDeprecated) {
          protocol.readEofPacket();
        }
      } else {
        if (numColumns > 0) {
          for (int i = 0; i < numColumns; i++) {
            columns[i] = new ColumnDefinition(reader.getPacket(false));
          }
          if (!eofDeprecated) {
            protocol.readEofPacket();
          }
        } else {
          // read warning only if no param / columns, because will be overwritten by EOF warning
          // data
          buffer.readByte(); // reserved
          protocol.setHasWarnings(buffer.readShort() > 0);
        }
      }

      ServerPrepareResult serverPrepareResult =
          new ServerPrepareResult(sql, statementId, columns, params, protocol);
      if (protocol.getOptions().cachePrepStmts
          && protocol.getOptions().useServerPrepStmts
          && sql != null
          && sql.length() < protocol.getOptions().prepStmtCacheSqlLimit) {
        String key = protocol.getDatabase() + "-" + sql;
        ServerPrepareResult cachedServerPrepareResult =
            protocol.addPrepareInCache(key, serverPrepareResult);
        return cachedServerPrepareResult != null ? cachedServerPrepareResult : serverPrepareResult;
      }
      return serverPrepareResult;

    } else {
      throw new SQLException("Unexpected packet returned by server, first byte " + firstByte);
    }
  }

  private SQLException buildErrorException(Buffer buffer) {
    ErrorPacket ep = new ErrorPacket(buffer);
    String message = ep.getMessage();
    if (1054 == ep.getErrorCode()) {
      return new SQLException(
          message
              + "\nIf column exists but type cannot be identified (example 'select ? `field1` from dual'). "
              + "Use CAST function to solve this problem (example 'select CAST(? as integer) `field1` from dual')",
          ep.getSqlState(),
          ep.getErrorCode());
    } else {
      return new SQLException(message, ep.getSqlState(), ep.getErrorCode());
    }
  }
}
