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

package org.mariadb.jdbc.internal.packet.result;

import org.mariadb.jdbc.internal.util.buffer.Buffer;

import java.nio.charset.StandardCharsets;


public class ErrorPacket extends AbstractResultPacket {
    private final short errorNumber;
    private final byte sqlStateMarker;
    private final byte[] sqlState;
    private final String message;

    /**
     * Reading error stream.
     * @param buffer current stream rawBytes
     */
    public ErrorPacket(Buffer buffer) {
        buffer.skipByte();
        this.errorNumber = buffer.readShort();
        this.sqlStateMarker = buffer.readByte();
        if (sqlStateMarker == '#') {
            this.sqlState = buffer.readRawBytes(5);
            this.message = buffer.readString(StandardCharsets.UTF_8);
        } else {
            // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
            this.message = new String(buffer.buf, buffer.position, buffer.limit, StandardCharsets.UTF_8);
            this.sqlState = "HY000".getBytes();
        }
    }

    /**
     * Constructor of error packet.
     * Permit to avoid first byte if already read.
     *
     * @param buffer buffer
     * @param firstByteToSkip has first byte is already read
     */
    public ErrorPacket(Buffer buffer, boolean firstByteToSkip) {
        if (firstByteToSkip) {
            buffer.skipByte();
        }
        this.errorNumber = buffer.readShort();
        this.sqlStateMarker = buffer.readByte();
        if (sqlStateMarker == '#') {
            this.sqlState = buffer.readRawBytes(5);
            this.message = buffer.readString(StandardCharsets.UTF_8);
        } else {
            // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
            this.message = new String(buffer.buf, buffer.position, buffer.limit, StandardCharsets.UTF_8);
            this.sqlState = "HY000".getBytes();
        }
    }

    public String getMessage() {
        return message;
    }

    public ResultType getResultType() {
        return ResultType.ERROR;
    }

    public byte getPacketSeq() {
        return 0;
        //return packetSeq;
    }

    public short getErrorNumber() {
        return errorNumber;
    }

    public String getSqlState() {
        return new String(sqlState);
    }

    public byte getSqlStateMarker() {
        return sqlStateMarker;
    }
}
