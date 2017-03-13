/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDB Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.packet;

import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.result.ErrorPacket;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.stream.PrepareException;
import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


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
     * @throws QueryException if packet max size is to big.
     */
    public void send(PacketOutputStream pos) throws IOException, QueryException {
        pos.startPacket(0);
        pos.write(Packet.COM_STMT_PREPARE);
        pos.write(this.sql);
        pos.flush();
    }

    /**
     * Read COM_PREPARE_RESULT.
     *
     * @param packetFetcher inputStream
     * @return ServerPrepareResult prepare result
     * @throws IOException is connection has error
     * @throws QueryException if server answer with error.
     */
    public ServerPrepareResult read(ReadPacketFetcher packetFetcher) throws IOException, QueryException {
        Buffer buffer = packetFetcher.getReusableBuffer();
        byte firstByte = buffer.getByteAt(0);

        if (firstByte == Packet.ERROR) {
            ErrorPacket ep = new ErrorPacket(buffer);
            String message = ep.getMessage();
            if (1054 == ep.getErrorNumber()) {
                throw new PrepareException("Error preparing query: " + message
                        + "\nIf column exists but type cannot be identified (example 'select ? `field1` from dual'). "
                        + "Use CAST function to solve this problem (example 'select CAST(? as integer) `field1` from dual')",
                        ep.getErrorNumber(), ep.getSqlState());
            } else {
                throw new PrepareException("Error preparing query: " + message, ep.getErrorNumber(), ep.getSqlState());
            }
        }

        if (firstByte == Packet.OK) {
                /* Prepared Statement OK */
            buffer.readByte(); /* skip field count */
            final int statementId = buffer.readInt();
            final int numColumns = buffer.readShort() & 0xffff;
            final int numParams = buffer.readShort() & 0xffff;

            ColumnInformation[] params = new ColumnInformation[numParams];
            ColumnInformation[] columns = new ColumnInformation[numColumns];

            if (numParams > 0) {
                for (int i = 0; i < numParams; i++) {
                    params[i] = new ColumnInformation(packetFetcher.getPacket());
                }

                if (numColumns > 0) {
                    protocol.skipEofPacket();
                    for (int i = 0; i < numColumns; i++) {
                        columns[i] = new ColumnInformation(packetFetcher.getPacket());
                    }
                }
                protocol.readEofPacket();
            } else {
                if (numColumns > 0) {
                    for (int i = 0; i < numColumns; i++) {
                        columns[i] = new ColumnInformation(packetFetcher.getPacket());
                    }
                    protocol.readEofPacket();
                } else {
                    //read warning only if no param / columns, because will be overwritten by EOF warning data
                    buffer.readByte(); // reserved
                    protocol.setHasWarnings(buffer.readShort() > 0);
                }
            }

            ServerPrepareResult serverPrepareResult = new ServerPrepareResult(sql, statementId, columns, params, protocol);
            if (protocol.getOptions().cachePrepStmts && sql != null && sql.length() < protocol.getOptions().prepStmtCacheSqlLimit) {
                String key = new StringBuilder(protocol.getDatabase()).append("-").append(sql).toString();
                ServerPrepareResult cachedServerPrepareResult = protocol.addPrepareInCache(key, serverPrepareResult);
                return cachedServerPrepareResult != null ? cachedServerPrepareResult : serverPrepareResult;
            }
            return serverPrepareResult;

        } else {
            throw new QueryException("Unexpected packet returned by server, first byte " + firstByte);
        }
    }

}