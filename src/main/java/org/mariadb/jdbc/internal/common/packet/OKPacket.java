/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

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

package org.mariadb.jdbc.internal.common.packet;

import org.mariadb.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 4:23:40 PM
 */
public class OKPacket extends ResultPacket {
    @SuppressWarnings("unused")
	private final byte fieldCount;
    private final long affectedRows;
    private final long insertId;
    private final short serverStatus;
    private final short warnings;
    private final String message;
    private final byte packetSeqNum;


    public OKPacket(final RawPacket rawPacket) throws IOException {
        Reader reader = new Reader(rawPacket);
        packetSeqNum = 0;
        fieldCount = reader.readByte();
        affectedRows = reader.getLengthEncodedBinary();
        insertId = reader.getLengthEncodedBinary();
        serverStatus = reader.readShort();
        warnings = reader.readShort();
        message = new String(reader.getLengthEncodedBytes());
    }

    public ResultType getResultType() {
        return ResultType.OK;
    }

    public byte getPacketSeq() {
        return packetSeqNum;
    }

    @Override
    public String toString() {
        return "affectedRows = " +
                affectedRows +
                "&insertId = " +
                insertId +
                "&serverStatus=" +
                serverStatus +
                "&warnings=" +
                warnings +
                "&message=" +
                message;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getInsertId() {
        return insertId;
    }

    public short getServerStatus() {
        return serverStatus;
    }

    public short getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }
}
