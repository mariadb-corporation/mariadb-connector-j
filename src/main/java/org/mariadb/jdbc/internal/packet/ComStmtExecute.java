/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson , Stephane Giron

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

import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.parameters.NullParameter;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.packet.send.InterfaceSendPacket;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.BulkStatus;

import java.io.IOException;
import java.io.OutputStream;

public class ComStmtExecute implements InterfaceSendPacket {
    private final int parameterCount;
    private final ParameterHolder[] parameters;
    private final int statementId;
    private MariaDbType[] parameterTypeHeader;

    /**
     * Initialize parameters.
     *
     * @param statementId         prepareResult object received after preparation.
     * @param parameters          parameters
     * @param parameterCount      parameters number
     * @param parameterTypeHeader parameters header
     */
    public ComStmtExecute(final int statementId, final ParameterHolder[] parameters, final int parameterCount,
                          MariaDbType[] parameterTypeHeader) {
        this.parameterCount = parameterCount;
        this.parameters = parameters;
        this.statementId = statementId;
        this.parameterTypeHeader = parameterTypeHeader;
    }

    /**
     * Send a prepare statement binary stream.
     *
     * @param os database socket
     * @throws IOException if a connection error occur
     */
    public void send(final OutputStream os) throws IOException {
        PacketOutputStream buffer = (PacketOutputStream) os;
        buffer.startPacket(0, true);
        writeCmd(statementId, parameters, parameterCount, parameterTypeHeader, buffer);
        buffer.finishPacket();
    }

    /**
     * Send COM_STMT_EXECUTE subcommand.
     *
     * @param statementId         prepareResult object received after preparation.
     * @param parameters          parameters
     * @param parameterCount      parameters count
     * @param parameterTypeHeader parameters header
     * @param writer outputStream
     * @param status bulk status
     * @return current buffer position
     * @throws IOException if a connection error occur
     */
    public static int sendComMulti(final int statementId, final ParameterHolder[] parameters, final int parameterCount,
                                    MariaDbType[] parameterTypeHeader, final PacketOutputStream writer, BulkStatus status)
            throws IOException {
        status.subCmdInitialPosition = writer.buffer.position();
        writer.assureBufferCapacity(3);
        writer.buffer.position(status.subCmdInitialPosition + 3);

        //add execute sub command
        writeCmd(statementId, parameters, parameterCount, parameterTypeHeader, writer);

        //write subCommand length
        int subCmdEndPosition = writer.buffer.position();
        writer.buffer.position(status.subCmdInitialPosition);
        writer.buffer.put((byte) ((subCmdEndPosition - (status.subCmdInitialPosition + 3)) & 0xff));
        writer.buffer.put((byte) ((subCmdEndPosition - (status.subCmdInitialPosition + 3)) >>> 8));
        writer.buffer.put((byte) ((subCmdEndPosition - (status.subCmdInitialPosition + 3)) >>> 16));
        writer.buffer.position(subCmdEndPosition);
        return subCmdEndPosition;

    }

    /**
     * Write COM_STMT_EXECUTE sub-command to outtput buffer.
     *
     * @param statementId         prepareResult object received after preparation.
     * @param parameters          parameters
     * @param parameterCount      parameters number
     * @param parameterTypeHeader parameters header1
     * @param pos outputStream
     * @throws IOException if a connection error occur
     */
    public static void writeCmd(final int statementId, final ParameterHolder[] parameters, final int parameterCount,
                                MariaDbType[] parameterTypeHeader, final PacketOutputStream pos) throws IOException {
        pos.write(Packet.COM_STMT_EXECUTE);
        pos.writeInt(statementId);
        pos.write((byte) 0x00); //CURSOR TYPE NO CURSOR
        pos.writeInt(1); //Iteration count

        //create null bitmap
        if (parameterCount > 0) {
            int nullCount = (parameterCount + 7) / 8;
            byte[] nullBitsBuffer = new byte[nullCount];
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i] instanceof NullParameter) {
                    nullBitsBuffer[i / 8] |= (1 << (i % 8));
                }
            }
            pos.write(nullBitsBuffer, 0, nullCount);/*Null Bit Map*/

            //check if parameters type (using setXXX) have change since previous request, and resend new header type if so
            boolean mustSendHeaderType = false;
            if (parameterCount == 0 || parameterTypeHeader[0] == null) {
                mustSendHeaderType = true;
            } else {
                for (int i = 0; i < parameterCount; i++) {
                    if (!parameterTypeHeader[i].equals(parameters[i].getMariaDbType())) {
                        mustSendHeaderType = true;
                        break;
                    }
                }
            }

            if (mustSendHeaderType) {
                pos.write((byte) 0x01);
                //Store types of parameters in first in first package that is sent to the server.
                for (int i = 0; i < parameterCount; i++) {
                    parameterTypeHeader[i] = parameters[i].getMariaDbType();
                    pos.writeShort((short) parameterTypeHeader[i].getType());
                }
            } else {
                pos.write((byte) 0x00);
            }
        }
        for (int i = 0; i < parameterCount; i++) {
            if (!parameters[i].isLongData()) parameters[i].writeBinary(pos);
        }
    }
}
